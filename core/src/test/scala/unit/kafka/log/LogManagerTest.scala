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
import kafka.server.metadata.{ConfigRepository, MockConfigRepository}
import kafka.utils._
import org.apache.directory.api.util.FileUtils
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.message.LeaderAndIsrRequestData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrTopicState
import org.apache.kafka.common.requests.{AbstractControlRequest, LeaderAndIsrRequest}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{DirectoryId, KafkaException, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.image.{TopicImage, TopicsImage}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.Mockito.{doAnswer, doNothing, mock, never, spy, times, verify}

import java.io._
import java.lang.{Long => JLong}
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Future}
import java.util.{Collections, Optional, OptionalLong, Properties}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.{FileLock, KafkaScheduler, MockTime, Scheduler}
import org.apache.kafka.storage.internals.log.{CleanerConfig, FetchDataInfo, LogConfig, LogDirFailureChannel, LogStartOffsetIncrementReason, ProducerStateManagerConfig, RemoteIndexCache}
import org.apache.kafka.storage.internals.checkpoint.{CleanShutdownFileHandler, OffsetCheckpointFile}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.function.Executable

import java.time.Duration
import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class LogManagerTest {
  import LogManagerTest._

  val time = new MockTime()
  val maxRollInterval = 100
  val maxLogAgeMs: Int = 10 * 60 * 1000
  val logProps = new Properties()
  logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, 1024: java.lang.Integer)
  logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 4096: java.lang.Integer)
  logProps.put(TopicConfig.RETENTION_MS_CONFIG, maxLogAgeMs: java.lang.Integer)
  val logConfig = new LogConfig(logProps)
  var logDir: File = _
  var logManager: LogManager = _
  val name = "kafka"
  val veryLargeLogFlushInterval = 10000000L
  val initialTaskDelayMs: Long = 10 * 1000

  @BeforeEach
  def setUp(): Unit = {
    logDir = TestUtils.tempDir()
    logManager = createLogManager()
    logManager.startup(Set.empty)
    assertEquals(initialTaskDelayMs, logManager.initialTaskDelayMs)
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
   * Test that getOrCreateLog on a non-existent log creates a new log in given logDirectory using directory id and that we can append to the new log.
   */
  @Test
  def testCreateLogOnTargetedLogDirectory(): Unit = {
    val targetedLogDirectoryId = DirectoryId.random()

    val dirs: Seq[File] = Seq.fill(5)(TestUtils.tempDir())
    writeMetaProperties(dirs.head)
    writeMetaProperties(dirs(1), Optional.of(targetedLogDirectoryId))
    writeMetaProperties(dirs(3), Optional.of(DirectoryId.random()))
    writeMetaProperties(dirs(4))

    logManager = createLogManager(dirs)

    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None, targetLogDirectoryId = Some(targetedLogDirectoryId))
    assertEquals(5, logManager.liveLogDirs.size)

    val logFile = new File(dirs(1), name + "-0")
    assertTrue(logFile.exists)
    assertEquals(dirs(1).getAbsolutePath, logFile.getParent)
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  /**
   * Test that getOrCreateLog on a non-existent log creates a new log in the next selected logDirectory if the given directory id is DirectoryId.UNASSIGNED.
   */
  @Test
  def testCreateLogWithTargetedLogDirectorySetAsUnassigned(): Unit = {
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None, targetLogDirectoryId = Some(DirectoryId.UNASSIGNED))
    assertEquals(1, logManager.liveLogDirs.size)
    val logFile = new File(logDir, name + "-0")
    assertTrue(logFile.exists)
    assertFalse(logManager.directoryId(logFile.getParent).equals(DirectoryId.UNASSIGNED))
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  @Test
  def testCreateLogWithTargetedLogDirectorySetAsUnknownWithoutAnyOfflineDirectories(): Unit = {

    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None, targetLogDirectoryId = Some(DirectoryId.LOST))
    assertEquals(1, logManager.liveLogDirs.size)
    val logFile = new File(logDir, name + "-0")
    assertTrue(logFile.exists)
    assertFalse(logManager.directoryId(logFile.getParent).equals(DirectoryId.random()))
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

      logManagerForTest.get.shutdown(3)

      assertFalse(Files.exists(new File(logDir1, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath))
      assertTrue(Files.exists(new File(logDir2, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath))
      assertEquals(OptionalLong.empty(), logManagerForTest.get.readBrokerEpochFromCleanShutdownFiles())
    } finally {
      logManagerForTest.foreach(manager => manager.liveLogDirs.foreach(Utils.delete))
    }
  }

  @Test
  def testCleanShutdownFileWithBrokerEpoch(): Unit = {
    val logDir1 = TestUtils.tempDir()
    val logDir2 = TestUtils.tempDir()
    var logManagerForTest: Option[LogManager] = Option.empty
    try {
      logManagerForTest = Some(createLogManager(Seq(logDir1, logDir2)))

      assertEquals(2, logManagerForTest.get.liveLogDirs.size)
      logManagerForTest.get.startup(Set.empty)
      logManagerForTest.get.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
      logManagerForTest.get.getOrCreateLog(new TopicPartition(name, 1), topicId = None)

      val logFile1 = new File(logDir1, name + "-0")
      assertTrue(logFile1.exists)
      val logFile2 = new File(logDir2, name + "-1")
      assertTrue(logFile2.exists)

      logManagerForTest.get.shutdown(3)

      assertTrue(Files.exists(new File(logDir1, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath))
      assertTrue(Files.exists(new File(logDir2, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath))
      assertEquals(OptionalLong.of(3L), logManagerForTest.get.readBrokerEpochFromCleanShutdownFiles())
    } finally {
      logManagerForTest.foreach(manager => manager.liveLogDirs.foreach(Utils.delete))
    }
  }

  /*
   * Test that LogManager.shutdown() doesn't create clean shutdown file for a log directory that has not completed
   * recovery.
   */
  @Test
  def testCleanShutdownFileWhenShutdownCalledBeforeStartupComplete(): Unit = {
    // 1. create two logs under logDir
    val topicPartition0 = new TopicPartition(name, 0)
    val topicPartition1 = new TopicPartition(name, 1)
    val log0 = logManager.getOrCreateLog(topicPartition0, topicId = None)
    val log1 = logManager.getOrCreateLog(topicPartition1, topicId = None)
    val logFile0 = new File(logDir, name + "-0")
    val logFile1 = new File(logDir, name + "-1")
    assertTrue(logFile0.exists)
    assertTrue(logFile1.exists)

    log0.appendAsLeader(TestUtils.singletonRecords("test1".getBytes()), leaderEpoch = 0)
    log0.takeProducerSnapshot()

    log1.appendAsLeader(TestUtils.singletonRecords("test1".getBytes()), leaderEpoch = 0)
    log1.takeProducerSnapshot()

    // 2. simulate unclean shutdown by deleting clean shutdown marker file
    logManager.shutdown()
    assertTrue(Files.deleteIfExists(new File(logDir, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath))

    // 3. create a new LogManager and start it in a different thread
    @volatile var loadLogCalled = 0
    logManager = spy(createLogManager())
    doAnswer { invocation =>
      // intercept LogManager.loadLog to sleep 5 seconds so that there is enough time to call LogManager.shutdown
      // before LogManager.startup completes.
      Thread.sleep(5000)
      invocation.callRealMethod().asInstanceOf[UnifiedLog]
      loadLogCalled = loadLogCalled + 1
    }.when(logManager).loadLog(any[File], any[Boolean], any[util.Map[TopicPartition, JLong]], any[util.Map[TopicPartition, JLong]],
      any[LogConfig], any[Map[String, LogConfig]], any[ConcurrentMap[String, Integer]], any[UnifiedLog => Boolean]())

    val t = new Thread() {
      override def run(): Unit = { logManager.startup(Set.empty) }
    }
    t.start()

    // 4. shutdown LogManager after the first log is loaded but before the second log is loaded
    TestUtils.waitUntilTrue(() => loadLogCalled == 1,
      "Timed out waiting for only the first log to be loaded")
    logManager.shutdown()
    logManager = null

    // 5. verify that CleanShutdownFile is not created under logDir
    assertFalse(Files.exists(new File(logDir, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath))
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
    for (_ <- 0 until 200) {
      val set = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.lastOffset
    }
    assertTrue(log.numberOfSegments > 1, "There should be more than one segment now.")
    log.updateHighWatermark(log.logEndOffset)

    log.logSegments.forEach(s => s.log.file.setLastModified(time.milliseconds))

    time.sleep(maxLogAgeMs + 1)
    assertEquals(1, log.numberOfSegments, "Now there should only be only one segment in the index.")
    time.sleep(log.config.fileDeleteDelayMs + 1)

    log.logSegments.forEach(s => {
      s.offsetIndex()
      s.timeIndex()
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
    properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, segmentBytes.toString)
    properties.put(TopicConfig.RETENTION_BYTES_CONFIG, (5L * 10L * setSize + 10L).toString)
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
      offset = info.firstOffset
    }

    log.updateHighWatermark(log.logEndOffset)
    assertEquals(numMessages * setSize / segmentBytes, log.numberOfSegments, "Check we have the expected number of segments.")

    // this cleanup shouldn't find any expired segments but should delete some to reduce size
    time.sleep(logManager.initialTaskDelayMs)
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
    testDoesntCleanLogs(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE)
  }

  /**
    * Ensures that LogManager doesn't run on logs with cleanup.policy=compact
    * LogCleaner.CleanerThread handles all logs where compaction is enabled.
    */
  @Test
  def testDoesntCleanLogsWithCompactPolicy(): Unit = {
    testDoesntCleanLogs(TopicConfig.CLEANUP_POLICY_COMPACT)
  }

  private def testDoesntCleanLogs(policy: String): Unit = {
    logManager.shutdown()
    val configRepository = MockConfigRepository.forTopic(name, TopicConfig.CLEANUP_POLICY_CONFIG, policy)

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

    log.logSegments.forEach(s => s.log.file.setLastModified(time.milliseconds))

    time.sleep(maxLogAgeMs + 1)
    assertEquals(numSegments, log.numberOfSegments, "number of segments shouldn't have changed")
  }

  /**
   * Test that flush is invoked by the background scheduler thread.
   */
  @Test
  def testTimeBasedFlush(): Unit = {
    logManager.shutdown()
    val configRepository = MockConfigRepository.forTopic(name, TopicConfig.FLUSH_MS_CONFIG, "1000")

    logManager = createLogManager(configRepository = configRepository)
    logManager.startup(Set.empty)
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
    val lastFlush = log.lastFlushTime
    for (_ <- 0 until 200) {
      val set = TestUtils.singletonRecords("test".getBytes())
      log.appendAsLeader(set, leaderEpoch = 0)
    }
    time.sleep(logManager.initialTaskDelayMs)
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
    for (partition <- 0 until 20) {
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
    val remoteIndexCache = new File(logDir, RemoteIndexCache.DIR_NAME)
    remoteIndexCache.mkdir()
    logManager = createLogManager(Seq(logDir))
    logManager.loadLogs(logConfig, Map.empty, _ => false)
  }

  @Test
  def testLoadLogRenameLogThatShouldBeStray(): Unit = {
    var invokedCount = 0
    val logDir = TestUtils.tempDir()
    logManager = createLogManager(Seq(logDir))

    val testTopic = "test-stray-topic"
    val testTopicPartition = new TopicPartition(testTopic, 0)
    val log = logManager.getOrCreateLog(testTopicPartition, topicId = Some(Uuid.randomUuid()))
    def providedIsStray(log: UnifiedLog) = {
      invokedCount += 1
      true
    }

    logManager.loadLog(log.dir, hadCleanShutdown = true, Collections.emptyMap[TopicPartition, JLong], Collections.emptyMap[TopicPartition, JLong], logConfig, Map.empty, new ConcurrentHashMap[String, Integer](),  providedIsStray)
    assertEquals(1, invokedCount)
    assertTrue(
      logDir.listFiles().toSet
      .exists(f => f.getName.startsWith(testTopic) && f.getName.endsWith(UnifiedLog.StrayDirSuffix))
    )
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
    val checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile), null).read()

    topicPartitions.zip(logs).foreach { case (tp, log) =>
      assertEquals(checkpoints.get(tp), log.recoveryPoint, "Recovery point should equal checkpoint")
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
      recoveryThreadsPerDataDir = recoveryThreadsPerDataDir,
      initialTaskDelayMs = initialTaskDelayMs)
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
    val indexFilesAfterDelete = Seq(removedSegment.offsetIndexFile, removedSegment.timeIndexFile,
      removedSegment.txnIndex.file)

    assertEquals(new File(removedLog.dir, logName), removedSegment.log.file)
    assertEquals(new File(removedLog.dir, indexName), removedSegment.offsetIndexFile)
    assertEquals(new File(removedLog.dir, timeIndexName), removedSegment.timeIndexFile)
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

    time.sleep(logManager.initialTaskDelayMs)
    assertTrue(logManager.hasLogsToBeDeleted, "Logs deleted too early")
    time.sleep(logManager.currentDefaultConfig.fileDeleteDelayMs - logManager.initialTaskDelayMs)
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

    val checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile), null).read()

    tps.zip(allLogs).foreach { case (tp, log) =>
      assertEquals(checkpoints.get(tp), log.recoveryPoint,
        "Recovery point should equal checkpoint")
    }
  }

  private def readLog(log: UnifiedLog, offset: Long, maxLength: Int = 1024): FetchDataInfo = {
    log.read(offset, maxLength, isolation = FetchIsolation.LOG_END, minOneMessage = true)
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
    oldProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    val oldLogConfig = LogConfig.fromProps(logConfig.originals, oldProperties)

    val log0 = spyLogManager.getOrCreateLog(tp0, topicId = None)
    log0.updateConfig(oldLogConfig)
    val log1 = spyLogManager.getOrCreateLog(tp1, topicId = None)
    log1.updateConfig(oldLogConfig)

    assertEquals(Set(log0, log1), spyLogManager.logsByTopic(topic).toSet)

    val newProperties = new Properties()
    newProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)

    spyLogManager.updateTopicConfig(topic, newProperties, isRemoteLogStorageSystemEnabled = false, wasRemoteLogEnabled = false, fromZK = false)

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
      5 * 60 * 1000, new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false), TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT)

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
    val logManagerClassName = classOf[LogManager].getSimpleName
    // get all `remainingLogsToRecover` metrics
    val logMetrics: ArrayBuffer[Gauge[Int]] = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (metric, _) => metric.getType == s"$logManagerClassName" && metric.getName == "remainingLogsToRecover" }
      .map { case (_, gauge) => gauge }
      .asInstanceOf[ArrayBuffer[Gauge[Int]]]

    assertEquals(expectedParams.size, logMetrics.size)

    val capturedPath: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    val expectedCallTimes = expectedParams.values.sum
    verify(spyLogManager, times(expectedCallTimes)).decNumRemainingLogs(any[ConcurrentMap[String, Int]], capturedPath.capture())

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
                                                     mockMap: ConcurrentHashMap[String, Integer],
                                                     expectedParams: Map[String, Int]): Unit = {
    val logManagerClassName = classOf[LogManager].getSimpleName
    // get all `remainingSegmentsToRecover` metrics
    val logSegmentMetrics: ArrayBuffer[Gauge[Int]] = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
          .filter { case (metric, _) => metric.getType == s"$logManagerClassName" && metric.getName == "remainingSegmentsToRecover" }
          .map { case (_, gauge) => gauge }
          .asInstanceOf[ArrayBuffer[Gauge[Int]]]

    // expected each log dir has 1 metrics for each thread
    assertEquals(recoveryThreadsPerDataDir * logDirs.size, logSegmentMetrics.size)

    val capturedThreadName: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val capturedNumRemainingSegments: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])

    // Since we'll update numRemainingSegments from totalSegments to 0 for each thread, so we need to add 1 here
    val expectedCallTimes = expectedParams.values.map( num => num + 1 ).sum
    verify(mockMap, times(expectedCallTimes)).put(capturedThreadName.capture(), capturedNumRemainingSegments.capture())

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
    val spyLogManagerClassName = spyLogManager.getClass.getSimpleName
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
    val mockMap = mock(classOf[ConcurrentHashMap[String, Integer]])
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
        producerStateManagerConfig = new ProducerStateManagerConfig(5 * 60 * 1000, false),
        producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
        scheduler = mock(classOf[Scheduler]),
        time = mockTime,
        brokerTopicStats = mockBrokerTopicStats,
        logDirFailureChannel = mock(classOf[LogDirFailureChannel]),
        // not clean shutdown
        lastShutdownClean = false,
        topicId = None,
        keepPartitionMetadataFile = false,
        // pass mock map for verification later
        numRemainingSegments = mockMap)

    }.when(spyLogManager).loadLog(any[File], any[Boolean], any[util.Map[TopicPartition, JLong]], any[util.Map[TopicPartition, JLong]],
      any[LogConfig], any[Map[String, LogConfig]], any[ConcurrentMap[String, Integer]], any[UnifiedLog => Boolean]())

    // do nothing for removeLogRecoveryMetrics for metrics verification
    doNothing().when(spyLogManager).removeLogRecoveryMetrics()

    // start the logManager to do log recovery
    spyLogManager.startup(Set.empty)

    // make sure log recovery metrics are added and removed
    verify(spyLogManager, times(1)).addLogRecoveryMetrics(any[ConcurrentMap[String, Int]], any[ConcurrentMap[String, Integer]])
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
    verify(spyLogManager, times(1)).addLogRecoveryMetrics(any[ConcurrentMap[String, Int]], any[ConcurrentMap[String, Integer]])
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

  @Test
  def testLoadDirectoryIds(): Unit = {
    val dirs: Seq[File] = Seq.fill(5)(TestUtils.tempDir())
    writeMetaProperties(dirs.head)
    writeMetaProperties(dirs(1), Optional.of(Uuid.fromString("ZwkGXjB0TvSF6mjVh6gO7Q")))
    // no meta.properties on dirs(2)
    writeMetaProperties(dirs(3), Optional.of(Uuid.fromString("kQfNPJ2FTHq_6Qlyyv6Jqg")))
    writeMetaProperties(dirs(4))

    logManager = createLogManager(dirs)

    assertFalse(logManager.directoryId(dirs.head.getAbsolutePath).isDefined)
    assertTrue(logManager.directoryId(dirs(1).getAbsolutePath).isDefined)
    assertEquals(Some(Uuid.fromString("ZwkGXjB0TvSF6mjVh6gO7Q")), logManager.directoryId(dirs(1).getAbsolutePath))
    assertEquals(None, logManager.directoryId(dirs(2).getAbsolutePath))
    assertEquals(Some(Uuid.fromString("kQfNPJ2FTHq_6Qlyyv6Jqg")), logManager.directoryId(dirs(3).getAbsolutePath))
    assertTrue(logManager.directoryId(dirs(3).getAbsolutePath).isDefined)
    assertEquals(2, logManager.directoryIdsSet.size)
  }

  @Test
  def testCheckpointLogStartOffsetForRemoteTopic(): Unit = {
    logManager.shutdown()

    val props = new Properties()
    props.putAll(logProps)
    props.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    val logConfig = new LogConfig(props)
    logManager = TestUtils.createLogManager(
      defaultConfig = logConfig,
      configRepository = new MockConfigRepository,
      logDirs = Seq(this.logDir),
      time = this.time,
      recoveryThreadsPerDataDir = 1,
      remoteStorageSystemEnable = true
    )

    val checkpointFile = new File(logDir, LogManager.LogStartOffsetCheckpointFile)
    val checkpoint = new OffsetCheckpointFile(checkpointFile, null)
    val topicPartition = new TopicPartition("test", 0)
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    var offset = 0L
    for(_ <- 0 until 50) {
      val set = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.lastOffset
      if (offset != 0 && offset % 10 == 0)
        log.roll()
    }
    assertEquals(5, log.logSegments.size())
    log.updateHighWatermark(49)
    // simulate calls to upload 3 segments to remote storage and remove them from local-log.
    log.updateHighestOffsetInRemoteStorage(30)
    log.maybeIncrementLocalLogStartOffset(31L, LogStartOffsetIncrementReason.SegmentDeletion)
    log.deleteOldSegments()
    assertEquals(2, log.logSegments.size())

    // simulate two remote-log segment deletion
    val logStartOffset = 21L
    log.maybeIncrementLogStartOffset(logStartOffset, LogStartOffsetIncrementReason.SegmentDeletion)
    logManager.checkpointLogStartOffsets()

    assertEquals(logStartOffset, log.logStartOffset)
    assertEquals(logStartOffset, checkpoint.read().getOrDefault(topicPartition, -1L))
  }

  @Test
  def testCheckpointLogStartOffsetForNormalTopic(): Unit = {
    val checkpointFile = new File(logDir, LogManager.LogStartOffsetCheckpointFile)
    val checkpoint = new OffsetCheckpointFile(checkpointFile, null)
    val topicPartition = new TopicPartition("test", 0)
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    var offset = 0L
    for(_ <- 0 until 50) {
      val set = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.lastOffset
      if (offset != 0 && offset % 10 == 0)
        log.roll()
    }
    assertEquals(5, log.logSegments.size())
    log.updateHighWatermark(49)

    val logStartOffset = 31L
    log.maybeIncrementLogStartOffset(logStartOffset, LogStartOffsetIncrementReason.SegmentDeletion)
    logManager.checkpointLogStartOffsets()
    assertEquals(5, log.logSegments.size())
    assertEquals(logStartOffset, checkpoint.read().getOrDefault(topicPartition, -1L))

    log.deleteOldSegments()
    assertEquals(2, log.logSegments.size())
    assertEquals(logStartOffset, log.logStartOffset)

    // When you checkpoint log-start-offset after removing the segments, then there should not be any checkpoint
    logManager.checkpointLogStartOffsets()
    assertEquals(-1L, checkpoint.read().getOrDefault(topicPartition, -1L))
  }

  def writeMetaProperties(dir: File, directoryId: Optional[Uuid] = Optional.empty()): Unit = {
    val metaProps = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V0).
      setClusterId("IVT1Seu3QjacxS7oBTKhDQ").
      setNodeId(1).
      setDirectoryId(directoryId).
      build()
    PropertiesUtils.writePropertiesFile(metaProps.toProperties,
      new File(dir, MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath, false)
  }

  val foo0 = new TopicIdPartition(Uuid.fromString("Sl08ZXU2QW6uF5hIoSzc8w"), new TopicPartition("foo", 0))
  val foo1 = new TopicIdPartition(Uuid.fromString("Sl08ZXU2QW6uF5hIoSzc8w"), new TopicPartition("foo", 1))
  val bar0 = new TopicIdPartition(Uuid.fromString("69O438ZkTSeqqclTtZO2KA"), new TopicPartition("bar", 0))
  val bar1 = new TopicIdPartition(Uuid.fromString("69O438ZkTSeqqclTtZO2KA"), new TopicPartition("bar", 1))
  val baz0 = new TopicIdPartition(Uuid.fromString("2Ik9_5-oRDOKpSXd2SuG5w"), new TopicPartition("baz", 0))
  val baz1 = new TopicIdPartition(Uuid.fromString("2Ik9_5-oRDOKpSXd2SuG5w"), new TopicPartition("baz", 1))
  val baz2 = new TopicIdPartition(Uuid.fromString("2Ik9_5-oRDOKpSXd2SuG5w"), new TopicPartition("baz", 2))
  val quux0 = new TopicIdPartition(Uuid.fromString("YS9owjv5TG2OlsvBM0Qw6g"), new TopicPartition("quux", 0))
  val recreatedFoo0 = new TopicIdPartition(Uuid.fromString("_dOOzPe3TfiWV21Lh7Vmqg"), new TopicPartition("foo", 0))
  val recreatedFoo1 = new TopicIdPartition(Uuid.fromString("_dOOzPe3TfiWV21Lh7Vmqg"), new TopicPartition("foo", 1))

  @Test
  def testIsStrayKraftReplicaWithEmptyImage(): Unit = {
    val image: TopicsImage = topicsImage(Seq())
    val onDisk = Seq(foo0, foo1, bar0, bar1, quux0).map(mockLog)
    assertTrue(onDisk.forall(log => LogManager.isStrayKraftReplica(0, image, log)))
  }

  @Test
  def testIsStrayKraftReplicaInImage(): Unit = {
    val image: TopicsImage = topicsImage(Seq(
      topicImage(Map(
        foo0 -> Seq(0, 1, 2),
      )),
      topicImage(Map(
        bar0 -> Seq(0, 1, 2),
        bar1 -> Seq(0, 1, 2),
      ))
    ))
    val onDisk = Seq(foo0, foo1, bar0, bar1, quux0).map(mockLog)
    val expectedStrays = Set(foo1, quux0).map(_.topicPartition())

    onDisk.foreach(log => assertEquals(expectedStrays.contains(log.topicPartition), LogManager.isStrayKraftReplica(0, image, log)))
  }

  @Test
  def testIsStrayKraftReplicaInImageWithRemoteReplicas(): Unit = {
    val image: TopicsImage = topicsImage(Seq(
      topicImage(Map(
        foo0 -> Seq(0, 1, 2),
      )),
      topicImage(Map(
        bar0 -> Seq(1, 2, 3),
        bar1 -> Seq(2, 3, 0),
      ))
    ))
    val onDisk = Seq(foo0, bar0, bar1).map(mockLog)
    val expectedStrays = Set(bar0).map(_.topicPartition)

    onDisk.foreach(log => assertEquals(expectedStrays.contains(log.topicPartition), LogManager.isStrayKraftReplica(0, image, log)))
  }

  @Test
  def testIsStrayKraftMissingTopicId(): Unit = {
    val log = Mockito.mock(classOf[UnifiedLog])
    Mockito.when(log.topicId).thenReturn(Option.empty)
    assertTrue(LogManager.isStrayKraftReplica(0, topicsImage(Seq()), log))
  }

  @Test
  def testFindStrayReplicasInEmptyLAIR(): Unit = {
    val onDisk = Seq(foo0, foo1, bar0, bar1, baz0, baz1, baz2, quux0)
    val expected = onDisk.map(_.topicPartition()).toSet
    assertEquals(expected,
      LogManager.findStrayReplicas(0,
        createLeaderAndIsrRequestForStrayDetection(Seq()),
          onDisk.map(mockLog)).toSet)
  }

  @Test
  def testFindNoStrayReplicasInFullLAIR(): Unit = {
    val onDisk = Seq(foo0, foo1, bar0, bar1, baz0, baz1, baz2, quux0)
    assertEquals(Set(),
      LogManager.findStrayReplicas(0,
      createLeaderAndIsrRequestForStrayDetection(onDisk),
        onDisk.map(mockLog)).toSet)
  }

  @Test
  def testFindSomeStrayReplicasInFullLAIR(): Unit = {
    val onDisk = Seq(foo0, foo1, bar0, bar1, baz0, baz1, baz2, quux0)
    val present = Seq(foo0, bar0, bar1, quux0)
    val expected = Seq(foo1, baz0, baz1, baz2).map(_.topicPartition()).toSet
    assertEquals(expected,
      LogManager.findStrayReplicas(0,
        createLeaderAndIsrRequestForStrayDetection(present),
        onDisk.map(mockLog)).toSet)
  }

  @Test
  def testTopicRecreationInFullLAIR(): Unit = {
    val onDisk = Seq(foo0, foo1, bar0, bar1, baz0, baz1, baz2, quux0)
    val present = Seq(recreatedFoo0, recreatedFoo1, bar0, baz0, baz1, baz2, quux0)
    val expected = Seq(foo0, foo1, bar1).map(_.topicPartition()).toSet
    assertEquals(expected,
      LogManager.findStrayReplicas(0,
        createLeaderAndIsrRequestForStrayDetection(present),
        onDisk.map(mockLog)).toSet)
  }

  /**
   * Test LogManager takes file lock by default and the lock is released after shutdown.
   */
  @Test
  def testLock(): Unit = {
    val tmpLogDir = TestUtils.tempDir()
    val tmpLogManager = createLogManager(Seq(tmpLogDir))

    try {
      // ${tmpLogDir}.lock is acquired by tmpLogManager
      val fileLock = new FileLock(new File(tmpLogDir, LogManager.LockFileName))
      assertFalse(fileLock.tryLock())
    } finally {
      // ${tmpLogDir}.lock is removed after shutdown
      tmpLogManager.shutdown()
      val f = new File(tmpLogDir, LogManager.LockFileName)
      assertFalse(f.exists())
    }
  }

  /**
   * Test KafkaScheduler can be shutdown when file delete delay is set to 0.
   */
  @Test
  def testShutdownWithZeroFileDeleteDelayMs(): Unit = {
    val tmpLogDir = TestUtils.tempDir()
    val tmpProperties = new Properties()
    tmpProperties.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "0")
    val scheduler = new KafkaScheduler(1, true, "log-manager-test")
    val tmpLogManager = new LogManager(logDirs = Seq(tmpLogDir).map(_.getAbsoluteFile),
      initialOfflineDirs = Array.empty[File],
      configRepository = new MockConfigRepository,
      initialDefaultConfig = new LogConfig(tmpProperties),
      cleanerConfig = new CleanerConfig(false),
      recoveryThreadsPerDataDir = 1,
      flushCheckMs = 1000L,
      flushRecoveryOffsetCheckpointMs = 10000L,
      flushStartOffsetCheckpointMs = 10000L,
      retentionCheckMs = 1000L,
      maxTransactionTimeoutMs = 5 * 60 * 1000,
      producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
      producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
      scheduler = scheduler,
      time = Time.SYSTEM,
      brokerTopicStats = new BrokerTopicStats,
      logDirFailureChannel = new LogDirFailureChannel(1),
      keepPartitionMetadataFile = true,
      interBrokerProtocolVersion = MetadataVersion.latestTesting,
      remoteStorageSystemEnable = false,
      initialTaskDelayMs = 0)

    scheduler.startup()
    tmpLogManager.startup(Set.empty)
    val stopLogManager: Executable = () => tmpLogManager.shutdown()
    val stopScheduler: Executable = () => scheduler.shutdown()
    assertTimeoutPreemptively(Duration.ofMillis(5000), stopLogManager)
    assertTimeoutPreemptively(Duration.ofMillis(5000), stopScheduler)
  }

  /**
   * This test simulates an offline log directory by removing write permissions from the directory.
   * It verifies that the LogManager continues to operate without failure in this scenario.
   * For more details, refer to KAFKA-17356.
   */
  @Test
  def testInvalidLogDirNotFailLogManager(): Unit = {
    logManager.shutdown()
    val permissions = Files.getPosixFilePermissions(logDir.toPath)
    // Remove write permissions for user, group, and others
    permissions.remove(PosixFilePermission.OWNER_WRITE)
    permissions.remove(PosixFilePermission.GROUP_WRITE)
    permissions.remove(PosixFilePermission.OTHERS_WRITE)
    Files.setPosixFilePermissions(logDir.toPath, permissions)

    try {
      logManager = assertDoesNotThrow(() => createLogManager())
      assertEquals(0, logManager.dirLocks.size)
    } finally {
      // Add write permissions back to make file cleanup passed
      permissions.add(PosixFilePermission.OWNER_WRITE)
      permissions.add(PosixFilePermission.GROUP_WRITE)
      permissions.add(PosixFilePermission.OTHERS_WRITE)
      Files.setPosixFilePermissions(logDir.toPath, permissions)
    }
  }
}

object LogManagerTest {
  def mockLog(
    topicIdPartition: TopicIdPartition
  ): UnifiedLog = {
    val log = Mockito.mock(classOf[UnifiedLog])
    Mockito.when(log.topicId).thenReturn(Some(topicIdPartition.topicId()))
    Mockito.when(log.topicPartition).thenReturn(topicIdPartition.topicPartition())
    log
  }

  def topicImage(
    partitions: Map[TopicIdPartition, Seq[Int]]
  ): TopicImage = {
    var topicName: String = null
    var topicId: Uuid = null
    partitions.keySet.foreach {
      partition => if (topicId == null) {
        topicId = partition.topicId()
      } else if (!topicId.equals(partition.topicId())) {
        throw new IllegalArgumentException("partition topic IDs did not match")
      }
        if (topicName == null) {
          topicName = partition.topic()
        } else if (!topicName.equals(partition.topic())) {
          throw new IllegalArgumentException("partition topic names did not match")
        }
    }
    if (topicId == null) {
      throw new IllegalArgumentException("Invalid empty partitions map.")
    }
    val partitionRegistrations = partitions.map { case (partition, replicas) =>
      Int.box(partition.partition()) -> new PartitionRegistration.Builder().
        setReplicas(replicas.toArray).
        setDirectories(DirectoryId.unassignedArray(replicas.size)).
        setIsr(replicas.toArray).
        setLeader(replicas.head).
        setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
        setLeaderEpoch(0).
        setPartitionEpoch(0).
        build()
    }
    new TopicImage(topicName, topicId, partitionRegistrations.asJava)
  }

  def topicsImage(
    topics: Seq[TopicImage]
  ): TopicsImage = {
    var retval = TopicsImage.EMPTY
    topics.foreach { t => retval = retval.including(t) }
    retval
  }

  def createLeaderAndIsrRequestForStrayDetection(
    partitions: Iterable[TopicIdPartition],
    leaders: Iterable[Int] = Seq(),
  ): LeaderAndIsrRequest = {
    val nextLeaderIter = leaders.iterator
    def nextLeader(): Int = {
      if (nextLeaderIter.hasNext) {
        nextLeaderIter.next()
      } else {
        3
      }
    }
    val data = new LeaderAndIsrRequestData().
      setControllerId(1000).
      setIsKRaftController(true).
      setType(AbstractControlRequest.Type.FULL.toByte)
    val topics = new java.util.LinkedHashMap[String, LeaderAndIsrTopicState]
    partitions.foreach(partition => {
      val topicState = topics.computeIfAbsent(partition.topic(),
        _ => new LeaderAndIsrTopicState().
          setTopicId(partition.topicId()).
          setTopicName(partition.topic()))
      topicState.partitionStates().add(new LeaderAndIsrRequestData.LeaderAndIsrPartitionState().
        setTopicName(partition.topic()).
        setPartitionIndex(partition.partition()).
        setControllerEpoch(123).
        setLeader(nextLeader()).
        setLeaderEpoch(456).
        setIsr(java.util.Arrays.asList(3, 4, 5)).
        setReplicas(java.util.Arrays.asList(3, 4, 5)).
        setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value()))
    })
    data.topicStates().addAll(topics.values())
    new LeaderAndIsrRequest(data, 7.toShort)
  }
}