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

import kafka.server.KafkaConfig
import kafka.server.metadata.MockConfigRepository
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.record.{ControlRecordType, DefaultRecordBatch, MemoryRecords, RecordBatch, RecordVersion, SimpleRecord, TimestampType}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.IBP_0_11_0_IV0
import org.apache.kafka.server.util.{MockTime, Scheduler}
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{AbortedTxn, CleanerConfig, EpochEntry, LocalLog, LogConfig, LogDirFailureChannel, LogFileUtils, LogLoader, LogOffsetMetadata, LogSegment, LogSegments, LogStartOffsetIncrementReason, OffsetIndex, ProducerStateManager, ProducerStateManagerConfig, SnapshotFile}
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertFalse, assertNotEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, reset, times, verify, when}

import java.io.{BufferedWriter, File, FileWriter, IOException}
import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.nio.file.{Files, NoSuchFileException, Paths}
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.{Optional, OptionalLong, Properties}
import scala.annotation.nowarn
import scala.collection.mutable.ListBuffer
import scala.collection.{Iterable, Map, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.{RichOption, RichOptional}

class LogLoaderTest {
  var config: KafkaConfig = _
  val brokerTopicStats = new BrokerTopicStats
  val maxTransactionTimeoutMs: Int = 5 * 60 * 1000
  val producerStateManagerConfig: ProducerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false)
  val producerIdExpirationCheckIntervalMs: Int = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  var logsToClose: Seq[UnifiedLog] = Seq()
  val mockTime = new MockTime()

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    config = KafkaConfig.fromProps(props)
  }

  @AfterEach
  def tearDown(): Unit = {
    brokerTopicStats.close()
    logsToClose.foreach(l => Utils.closeQuietly(l, "UnifiedLog"))
    Utils.delete(tmpDir)
  }

  object ErrorTypes extends Enumeration {
    type Errors = Value
    val IOException, RuntimeException, KafkaStorageExceptionWithIOExceptionCause,
    KafkaStorageExceptionWithoutIOExceptionCause = Value
  }

  @Test
  def testLogRecoveryIsCalledUponBrokerCrash(): Unit = {
    // LogManager must realize correctly if the last shutdown was not clean and the logs need
    // to run recovery while loading upon subsequent broker boot up.
    val logDir: File = TestUtils.tempDir()
    val logProps = new Properties()
    val logConfig = new LogConfig(logProps)
    val logDirs = Seq(logDir)
    val topicPartition = new TopicPartition("foo", 0)
    var log: UnifiedLog = null
    val time = new MockTime()
    var cleanShutdownInterceptedValue = false
    case class SimulateError(var hasError: Boolean = false, var errorType: ErrorTypes.Errors = ErrorTypes.RuntimeException)
    val simulateError = SimulateError()
    val logDirFailureChannel = new LogDirFailureChannel(logDirs.size)

    val maxTransactionTimeoutMs = 5 * 60 * 1000
    val producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT

    // Create a LogManager with some overridden methods to facilitate interception of clean shutdown
    // flag and to inject an error
    def interceptedLogManager(logConfig: LogConfig,
                              logDirs: Seq[File],
                              logDirFailureChannel: LogDirFailureChannel
                             ): LogManager = {
      new LogManager(
        logDirs = logDirs.map(_.getAbsoluteFile),
        initialOfflineDirs = Array.empty[File],
        configRepository = new MockConfigRepository(),
        initialDefaultConfig = logConfig,
        cleanerConfig = new CleanerConfig(false),
        recoveryThreadsPerDataDir = 4,
        flushCheckMs = 1000L,
        flushRecoveryOffsetCheckpointMs = 10000L,
        flushStartOffsetCheckpointMs = 10000L,
        retentionCheckMs = 1000L,
        maxTransactionTimeoutMs = maxTransactionTimeoutMs,
        producerStateManagerConfig = producerStateManagerConfig,
        producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
        interBrokerProtocolVersion = config.interBrokerProtocolVersion,
        scheduler = time.scheduler,
        brokerTopicStats = new BrokerTopicStats(),
        logDirFailureChannel = logDirFailureChannel,
        time = time,
        keepPartitionMetadataFile = config.usesTopicId,
        remoteStorageSystemEnable = config.remoteLogManagerConfig.isRemoteStorageSystemEnabled(),
        initialTaskDelayMs = config.logInitialTaskDelayMs) {

        override def loadLog(logDir: File, hadCleanShutdown: Boolean, recoveryPoints: util.Map[TopicPartition, JLong],
                             logStartOffsets: util.Map[TopicPartition, JLong], defaultConfig: LogConfig,
                             topicConfigs: Map[String, LogConfig], numRemainingSegments: ConcurrentMap[String, Integer],
                             shouldBeStrayKraftLog: UnifiedLog => Boolean): UnifiedLog = {
          if (simulateError.hasError) {
            simulateError.errorType match {
              case ErrorTypes.KafkaStorageExceptionWithIOExceptionCause =>
                throw new KafkaStorageException(new IOException("Simulated Kafka storage error with IOException cause"))
              case ErrorTypes.KafkaStorageExceptionWithoutIOExceptionCause =>
                throw new KafkaStorageException("Simulated Kafka storage error without IOException cause")
              case ErrorTypes.IOException =>
                throw new IOException("Simulated IO error")
              case _ =>
                throw new RuntimeException("Simulated Runtime error")
            }
          }
          cleanShutdownInterceptedValue = hadCleanShutdown
          val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
          val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
          val logRecoveryPoint = recoveryPoints.getOrDefault(topicPartition, 0L)
          val logStartOffset = logStartOffsets.getOrDefault(topicPartition, 0L)
          val logDirFailureChannel: LogDirFailureChannel = new LogDirFailureChannel(1)
          val segments = new LogSegments(topicPartition)
          val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
            logDir, topicPartition, logDirFailureChannel, config.recordVersion, "", None, time.scheduler)
          val producerStateManager = new ProducerStateManager(topicPartition, logDir,
            this.maxTransactionTimeoutMs, this.producerStateManagerConfig, time)
          val logLoader = new LogLoader(logDir, topicPartition, config, time.scheduler, time,
            logDirFailureChannel, hadCleanShutdown, segments, logStartOffset, logRecoveryPoint,
            leaderEpochCache.toJava, producerStateManager, new ConcurrentHashMap[String, Integer], false)
          val offsets = logLoader.load()
          val localLog = new LocalLog(logDir, logConfig, segments, offsets.recoveryPoint,
            offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, topicPartition,
            logDirFailureChannel)
          new UnifiedLog(offsets.logStartOffset, localLog, brokerTopicStats,
            this.producerIdExpirationCheckIntervalMs, leaderEpochCache,
            producerStateManager, None, true)
        }
      }
    }

    def initializeLogManagerForSimulatingErrorTest(logDirFailureChannel: LogDirFailureChannel = new LogDirFailureChannel(logDirs.size)
                                                  ): (LogManager, Executable) = {
      val logManager: LogManager = interceptedLogManager(logConfig, logDirs, logDirFailureChannel)
      log = logManager.getOrCreateLog(topicPartition, isNew = true, topicId = None)

      assertFalse(logDirFailureChannel.hasOfflineLogDir(logDir.getAbsolutePath), "log dir should not be offline before load logs")

      val runLoadLogs: Executable = () => {
        val defaultConfig = logManager.currentDefaultConfig
        logManager.loadLogs(defaultConfig, logManager.fetchTopicConfigOverrides(defaultConfig, Set.empty), _ => false)
      }

      (logManager, runLoadLogs)
    }

    val cleanShutdownFileHandler = new CleanShutdownFileHandler(logDir.getPath)
    locally {
      val (logManager, _) = initializeLogManagerForSimulatingErrorTest()

      // Load logs after a clean shutdown
      cleanShutdownFileHandler.write(0L)
      cleanShutdownInterceptedValue = false
      var defaultConfig = logManager.currentDefaultConfig
      logManager.loadLogs(defaultConfig, logManager.fetchTopicConfigOverrides(defaultConfig, Set.empty), _ => false)
      assertTrue(cleanShutdownInterceptedValue, "Unexpected value intercepted for clean shutdown flag")
      assertFalse(cleanShutdownFileHandler.exists(), "Clean shutdown file must not exist after loadLogs has completed")
      // Load logs without clean shutdown file
      cleanShutdownInterceptedValue = true
      defaultConfig = logManager.currentDefaultConfig
      logManager.loadLogs(defaultConfig, logManager.fetchTopicConfigOverrides(defaultConfig, Set.empty), _ => false)
      assertFalse(cleanShutdownInterceptedValue, "Unexpected value intercepted for clean shutdown flag")
      assertFalse(cleanShutdownFileHandler.exists(), "Clean shutdown file must not exist after loadLogs has completed")
      // Create clean shutdown file and then simulate error while loading logs such that log loading does not complete.
      cleanShutdownFileHandler.write(0L)
      logManager.shutdown()
    }

    locally {
      val (logManager, runLoadLogs) = initializeLogManagerForSimulatingErrorTest(logDirFailureChannel)

      // Simulate Runtime error
      simulateError.hasError = true
      simulateError.errorType = ErrorTypes.RuntimeException
      assertThrows(classOf[RuntimeException], runLoadLogs)
      assertFalse(cleanShutdownFileHandler.exists(), "Clean shutdown file must not have existed")
      assertFalse(logDirFailureChannel.hasOfflineLogDir(logDir.getAbsolutePath), "log dir should not turn offline when Runtime Exception thrown")

      // Simulate Kafka storage error with IOException cause
      // in this case, the logDir will be added into offline list before KafkaStorageThrown. So we don't verify it here
      simulateError.errorType = ErrorTypes.KafkaStorageExceptionWithIOExceptionCause
      assertDoesNotThrow(runLoadLogs, "KafkaStorageException with IOException cause should be caught and handled")

      // Simulate Kafka storage error without IOException cause
      simulateError.errorType = ErrorTypes.KafkaStorageExceptionWithoutIOExceptionCause
      assertThrows(classOf[KafkaStorageException], runLoadLogs, "should throw exception when KafkaStorageException without IOException cause")
      assertFalse(logDirFailureChannel.hasOfflineLogDir(logDir.getAbsolutePath), "log dir should not turn offline when KafkaStorageException without IOException cause thrown")

      // Simulate IO error
      simulateError.errorType = ErrorTypes.IOException
      assertDoesNotThrow(runLoadLogs, "IOException should be caught and handled")
      assertTrue(logDirFailureChannel.hasOfflineLogDir(logDir.getAbsolutePath), "the log dir should turn offline after IOException thrown")

      // Do not simulate error on next call to LogManager#loadLogs. LogManager must understand that log had unclean shutdown the last time.
      simulateError.hasError = false
      cleanShutdownInterceptedValue = true
      val defaultConfig = logManager.currentDefaultConfig
      logManager.loadLogs(defaultConfig, logManager.fetchTopicConfigOverrides(defaultConfig, Set.empty), _ => false)
      assertFalse(cleanShutdownInterceptedValue, "Unexpected value for clean shutdown flag")
      logManager.shutdown()
    }
  }

  @Test
  def testProducerSnapshotsRecoveryAfterUncleanShutdownV1(): Unit = {
    testProducerSnapshotsRecoveryAfterUncleanShutdown(MetadataVersion.minSupportedFor(RecordVersion.V1).version)
  }

  @Test
  def testProducerSnapshotsRecoveryAfterUncleanShutdownCurrentMessageFormat(): Unit = {
    testProducerSnapshotsRecoveryAfterUncleanShutdown(MetadataVersion.latestTesting.version)
  }

  private def createLog(dir: File,
                        config: LogConfig,
                        brokerTopicStats: BrokerTopicStats = brokerTopicStats,
                        logStartOffset: Long = 0L,
                        recoveryPoint: Long = 0L,
                        scheduler: Scheduler = mockTime.scheduler,
                        time: Time = mockTime,
                        maxTransactionTimeoutMs: Int = maxTransactionTimeoutMs,
                        maxProducerIdExpirationMs: Int = producerStateManagerConfig.producerIdExpirationMs,
                        producerIdExpirationCheckIntervalMs: Int = producerIdExpirationCheckIntervalMs,
                        lastShutdownClean: Boolean = true): UnifiedLog = {
    val log = LogTestUtils.createLog(dir, config, brokerTopicStats, scheduler, time, logStartOffset, recoveryPoint,
      maxTransactionTimeoutMs, new ProducerStateManagerConfig(maxProducerIdExpirationMs, false), producerIdExpirationCheckIntervalMs, lastShutdownClean)
    logsToClose = logsToClose :+ log
    log
  }

  private def createLogWithOffsetOverflow(logConfig: LogConfig): (UnifiedLog, LogSegment) = {
    LogTestUtils.initializeLogDirWithOverflowedSegment(logDir)

    val log = createLog(logDir, logConfig, recoveryPoint = Long.MaxValue)
    val segmentWithOverflow = LogTestUtils.firstOverflowSegment(log).getOrElse {
      throw new AssertionError("Failed to create log with a segment which has overflowed offsets")
    }

    (log, segmentWithOverflow)
  }

  private def recoverAndCheck(config: LogConfig, expectedKeys: Iterable[Long]): UnifiedLog = {
    // method is called only in case of recovery from hard reset
    val recoveredLog = LogTestUtils.recoverAndCheck(logDir, config, expectedKeys, brokerTopicStats, mockTime, mockTime.scheduler)
    logsToClose = logsToClose :+ recoveredLog
    recoveredLog
  }

  /**
   * Wrap a single record log buffer with leader epoch.
   */
  private def singletonRecordsWithLeaderEpoch(value: Array[Byte],
                                              key: Array[Byte] = null,
                                              leaderEpoch: Int,
                                              offset: Long,
                                              codec: Compression = Compression.NONE,
                                              timestamp: Long = RecordBatch.NO_TIMESTAMP,
                                              magicValue: Byte = RecordBatch.CURRENT_MAGIC_VALUE): MemoryRecords = {
    val records = Seq(new SimpleRecord(timestamp, key, value))

    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, offset,
      mockTime.milliseconds, leaderEpoch)
    records.foreach(builder.append)
    builder.build()
  }

  @nowarn("cat=deprecation")
  private def testProducerSnapshotsRecoveryAfterUncleanShutdown(messageFormatVersion: String): Unit = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, "640")
    logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, messageFormatVersion)
    val logConfig = new LogConfig(logProps)
    var log = createLog(logDir, logConfig)
    assertEquals(OptionalLong.empty(), log.oldestProducerSnapshotOffset)

    for (i <- 0 to 100) {
      val record = new SimpleRecord(mockTime.milliseconds, i.toString.getBytes)
      log.appendAsLeader(TestUtils.records(List(record)), leaderEpoch = 0)
    }

    assertTrue(log.logSegments.size >= 5)
    val segmentOffsets = log.logSegments.asScala.toVector.map(_.baseOffset)
    val activeSegmentOffset = segmentOffsets.last

    // We want the recovery point to be past the segment offset and before the last 2 segments including a gap of
    // 1 segment. We collect the data before closing the log.
    val offsetForSegmentAfterRecoveryPoint = segmentOffsets(segmentOffsets.size - 3)
    val offsetForRecoveryPointSegment = segmentOffsets(segmentOffsets.size - 4)
    val (segOffsetsBeforeRecovery, segOffsetsAfterRecovery) = segmentOffsets.toSet.partition(_ < offsetForRecoveryPointSegment)
    val recoveryPoint = offsetForRecoveryPointSegment + 1
    assertTrue(recoveryPoint < offsetForSegmentAfterRecoveryPoint)
    log.close()

    val segmentsWithReads = mutable.Set[LogSegment]()
    val recoveredSegments = mutable.Set[LogSegment]()
    val expectedSegmentsWithReads = mutable.Set[Long]()
    val expectedSnapshotOffsets = mutable.Set[Long]()

    if (logConfig.messageFormatVersion.isLessThan(IBP_0_11_0_IV0)) {
      expectedSegmentsWithReads += activeSegmentOffset
      expectedSnapshotOffsets ++= log.logSegments.asScala.map(_.baseOffset).toVector.takeRight(2) :+ log.logEndOffset
    } else {
      expectedSegmentsWithReads ++= segOffsetsBeforeRecovery ++ Set(activeSegmentOffset)
      expectedSnapshotOffsets ++= log.logSegments.asScala.map(_.baseOffset).toVector.takeRight(4) :+ log.logEndOffset
    }

    def createLogWithInterceptedReads(recoveryPoint: Long): UnifiedLog = {
      val maxTransactionTimeoutMs = 5 * 60 * 1000
      val producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT
      val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
      val logDirFailureChannel = new LogDirFailureChannel(10)
      // Intercept all segment read calls
      val interceptedLogSegments = new LogSegments(topicPartition) {
        override def add(segment: LogSegment): LogSegment = {
          val wrapper = Mockito.spy(segment)
          Mockito.doAnswer { in =>
            segmentsWithReads += wrapper
            segment.read(in.getArgument(0, classOf[java.lang.Long]), in.getArgument(1, classOf[java.lang.Integer]), in.getArgument(2, classOf[java.util.Optional[java.lang.Long]]), in.getArgument(3, classOf[java.lang.Boolean]))
          }.when(wrapper).read(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())
          Mockito.doAnswer { in =>
            recoveredSegments += wrapper
            segment.recover(in.getArgument(0, classOf[ProducerStateManager]), in.getArgument(1, classOf[Optional[LeaderEpochFileCache]]))
          }.when(wrapper).recover(ArgumentMatchers.any(), ArgumentMatchers.any())
          super.add(wrapper)
        }
      }
      val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
        logDir, topicPartition, logDirFailureChannel, logConfig.recordVersion, "", None, mockTime.scheduler)
      val producerStateManager = new ProducerStateManager(topicPartition, logDir,
        maxTransactionTimeoutMs, producerStateManagerConfig, mockTime)
      val logLoader = new LogLoader(
        logDir,
        topicPartition,
        logConfig,
        mockTime.scheduler,
        mockTime,
        logDirFailureChannel,
        false,
        interceptedLogSegments,
        0L,
        recoveryPoint,
        leaderEpochCache.toJava,
        producerStateManager,
        new ConcurrentHashMap[String, Integer],
        false
      )
      val offsets = logLoader.load()
      val localLog = new LocalLog(logDir, logConfig, interceptedLogSegments, offsets.recoveryPoint,
        offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, topicPartition,
        logDirFailureChannel)
      new UnifiedLog(offsets.logStartOffset, localLog, brokerTopicStats,
        producerIdExpirationCheckIntervalMs, leaderEpochCache, producerStateManager,
        None, keepPartitionMetadataFile = true)
    }

    // Retain snapshots for the last 2 segments
    log.producerStateManager.deleteSnapshotsBefore(segmentOffsets(segmentOffsets.size - 2))
    log = createLogWithInterceptedReads(offsetForRecoveryPointSegment)
    // We will reload all segments because the recovery point is behind the producer snapshot files (pre KAFKA-5829 behaviour)
    assertEquals(expectedSegmentsWithReads, segmentsWithReads.map(_.baseOffset))
    assertEquals(segOffsetsAfterRecovery, recoveredSegments.map(_.baseOffset))
    assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir).toSet)
    log.close()
    segmentsWithReads.clear()
    recoveredSegments.clear()

    // Only delete snapshots before the base offset of the recovery point segment (post KAFKA-5829 behaviour) to
    // avoid reading all segments
    log.producerStateManager.deleteSnapshotsBefore(offsetForRecoveryPointSegment)
    log = createLogWithInterceptedReads(recoveryPoint = recoveryPoint)
    assertEquals(Set(activeSegmentOffset), segmentsWithReads.map(_.baseOffset))
    assertEquals(segOffsetsAfterRecovery, recoveredSegments.map(_.baseOffset))
    assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir).toSet)

    log.close()
  }

  @Test
  def testSkipLoadingIfEmptyProducerStateBeforeTruncation(): Unit = {
    val maxTransactionTimeoutMs = 60000
    val producerStateManagerConfig = new ProducerStateManagerConfig(300000, false)

    val stateManager: ProducerStateManager = mock(classOf[ProducerStateManager])
    when(stateManager.producerStateManagerConfig).thenReturn(producerStateManagerConfig)
    when(stateManager.maxTransactionTimeoutMs).thenReturn(maxTransactionTimeoutMs)
    when(stateManager.latestSnapshotOffset).thenReturn(OptionalLong.empty())
    when(stateManager.mapEndOffset).thenReturn(0L)
    when(stateManager.isEmpty).thenReturn(true)
    when(stateManager.firstUnstableOffset).thenReturn(Optional.empty[LogOffsetMetadata]())

    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    val logDirFailureChannel: LogDirFailureChannel = new LogDirFailureChannel(1)
    val config = new LogConfig(new Properties())
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
      logDir, topicPartition, logDirFailureChannel, config.recordVersion, "", None, mockTime.scheduler)
    val offsets = new LogLoader(
      logDir,
      topicPartition,
      config,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      false,
      segments,
      0L,
      0L,
      leaderEpochCache.toJava,
      stateManager,
      new ConcurrentHashMap[String, Integer],
      false
    ).load()
    val localLog = new LocalLog(logDir, config, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, topicPartition,
      logDirFailureChannel)
    val log = new UnifiedLog(offsets.logStartOffset,
      localLog,
      brokerTopicStats = brokerTopicStats,
      producerIdExpirationCheckIntervalMs = 30000,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = stateManager,
      _topicId = None,
      keepPartitionMetadataFile = true)

    verify(stateManager).updateMapEndOffset(0L)
    verify(stateManager).removeStraySnapshots(any())
    verify(stateManager).takeSnapshot()
    verify(stateManager).truncateAndReload(ArgumentMatchers.eq(0L), ArgumentMatchers.eq(0L), anyLong)

    // Append some messages
    reset(stateManager)
    when(stateManager.firstUnstableOffset).thenReturn(Optional.empty[LogOffsetMetadata]())

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes))), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes))), leaderEpoch = 0)

    verify(stateManager).updateMapEndOffset(1L)
    verify(stateManager).updateMapEndOffset(2L)

    // Now truncate
    reset(stateManager)
    when(stateManager.firstUnstableOffset).thenReturn(Optional.empty[LogOffsetMetadata]())
    when(stateManager.latestSnapshotOffset).thenReturn(OptionalLong.empty())
    when(stateManager.isEmpty).thenReturn(true)
    when(stateManager.mapEndOffset).thenReturn(2L)
    // Truncation causes the map end offset to reset to 0
    when(stateManager.mapEndOffset).thenReturn(0L)

    log.truncateTo(1L)

    verify(stateManager).truncateAndReload(ArgumentMatchers.eq(0L), ArgumentMatchers.eq(1L), anyLong)
    verify(stateManager).updateMapEndOffset(1L)
    verify(stateManager, times(2)).takeSnapshot()
  }

  @Test
  def testRecoverAfterNonMonotonicCoordinatorEpochWrite(): Unit = {
    // Due to KAFKA-9144, we may encounter a coordinator epoch which goes backwards.
    // This test case verifies that recovery logic relaxes validation in this case and
    // just takes the latest write.

    val producerId = 1L
    val coordinatorEpoch = 5
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    var log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val firstAppendTimestamp = mockTime.milliseconds()
    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, ControlRecordType.ABORT,
      firstAppendTimestamp, coordinatorEpoch = coordinatorEpoch)
    assertEquals(firstAppendTimestamp, log.producerStateManager.lastEntry(producerId).get.lastTimestamp)

    val maxProducerIdExpirationMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT
    mockTime.sleep(maxProducerIdExpirationMs)
    assertEquals(Optional.empty(), log.producerStateManager.lastEntry(producerId))

    val secondAppendTimestamp = mockTime.milliseconds()
    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, ControlRecordType.ABORT,
      secondAppendTimestamp, coordinatorEpoch = coordinatorEpoch - 1)

    log.close()

    // Force recovery by setting the recoveryPoint to the log start
    log = createLog(logDir, logConfig, recoveryPoint = 0L, lastShutdownClean = false)
    assertEquals(secondAppendTimestamp, log.producerStateManager.lastEntry(producerId).get.lastTimestamp)
    log.close()
  }

  @nowarn("cat=deprecation")
  @Test
  def testSkipTruncateAndReloadIfOldMessageFormatAndNoCleanShutdown(): Unit = {
    val maxTransactionTimeoutMs = 60000
    val producerStateManagerConfig = new ProducerStateManagerConfig(300000, false)

    val stateManager: ProducerStateManager = mock(classOf[ProducerStateManager])
    when(stateManager.isEmpty).thenReturn(true)
    when(stateManager.firstUnstableOffset).thenReturn(Optional.empty[LogOffsetMetadata]())
    when(stateManager.producerStateManagerConfig).thenReturn(producerStateManagerConfig)
    when(stateManager.maxTransactionTimeoutMs).thenReturn(maxTransactionTimeoutMs)

    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    val logProps = new Properties()
    logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "0.10.2")
    val config = new LogConfig(logProps)
    val logDirFailureChannel = null
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
      logDir, topicPartition, logDirFailureChannel, config.recordVersion, "", None, mockTime.scheduler)
    val offsets = new LogLoader(
      logDir,
      topicPartition,
      config,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      false,
      segments,
      0L,
      0L,
      leaderEpochCache.toJava,
      stateManager,
      new ConcurrentHashMap[String, Integer],
      false
    ).load()
    val localLog = new LocalLog(logDir, config, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, topicPartition,
      logDirFailureChannel)
    new UnifiedLog(offsets.logStartOffset,
      localLog,
      brokerTopicStats = brokerTopicStats,
      producerIdExpirationCheckIntervalMs = 30000,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = stateManager,
      _topicId = None,
      keepPartitionMetadataFile = true)

    verify(stateManager).removeStraySnapshots(any[java.util.List[java.lang.Long]])
    verify(stateManager, times(2)).updateMapEndOffset(0L)
    verify(stateManager, times(2)).takeSnapshot()
    verify(stateManager).isEmpty
    verify(stateManager).firstUnstableOffset
    verify(stateManager, times(2)).takeSnapshot()
    verify(stateManager, times(2)).updateMapEndOffset(0L)
  }

  @nowarn("cat=deprecation")
  @Test
  def testSkipTruncateAndReloadIfOldMessageFormatAndCleanShutdown(): Unit = {
    val maxTransactionTimeoutMs = 60000
    val producerStateManagerConfig = new ProducerStateManagerConfig(300000, false)

    val stateManager: ProducerStateManager = mock(classOf[ProducerStateManager])
    when(stateManager.isEmpty).thenReturn(true)
    when(stateManager.firstUnstableOffset).thenReturn(Optional.empty[LogOffsetMetadata]())
    when(stateManager.producerStateManagerConfig).thenReturn(producerStateManagerConfig)
    when(stateManager.maxTransactionTimeoutMs).thenReturn(maxTransactionTimeoutMs)

    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    val logProps = new Properties()
    logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "0.10.2")
    val config = new LogConfig(logProps)
    val logDirFailureChannel = null
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
      logDir, topicPartition, logDirFailureChannel, config.recordVersion, "", None, mockTime.scheduler)
    val offsets = new LogLoader(
      logDir,
      topicPartition,
      config,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      true,
      segments,
      0L,
      0L,
      leaderEpochCache.toJava,
      stateManager,
      new ConcurrentHashMap[String, Integer],
      false
    ).load()
    val localLog = new LocalLog(logDir, config, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, topicPartition,
      logDirFailureChannel)
    new UnifiedLog(offsets.logStartOffset,
      localLog,
      brokerTopicStats = brokerTopicStats,
      producerIdExpirationCheckIntervalMs = 30000,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = stateManager,
      _topicId = None,
      keepPartitionMetadataFile = true)

    verify(stateManager).removeStraySnapshots(any[java.util.List[java.lang.Long]])
    verify(stateManager, times(2)).updateMapEndOffset(0L)
    verify(stateManager, times(2)).takeSnapshot()
    verify(stateManager).isEmpty
    verify(stateManager).firstUnstableOffset
  }

  @nowarn("cat=deprecation")
  @Test
  def testSkipTruncateAndReloadIfNewMessageFormatAndCleanShutdown(): Unit = {
    val maxTransactionTimeoutMs = 60000
    val producerStateManagerConfig = new ProducerStateManagerConfig(300000, false)

    val stateManager: ProducerStateManager = mock(classOf[ProducerStateManager])
    when(stateManager.latestSnapshotOffset).thenReturn(OptionalLong.empty())
    when(stateManager.isEmpty).thenReturn(true)
    when(stateManager.firstUnstableOffset).thenReturn(Optional.empty[LogOffsetMetadata]())
    when(stateManager.producerStateManagerConfig).thenReturn(producerStateManagerConfig)
    when(stateManager.maxTransactionTimeoutMs).thenReturn(maxTransactionTimeoutMs)

    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    val logProps = new Properties()
    logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "0.11.0")
    val config = new LogConfig(logProps)
    val logDirFailureChannel = null
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
      logDir, topicPartition, logDirFailureChannel, config.recordVersion, "", None, mockTime.scheduler)
    val offsets = new LogLoader(
      logDir,
      topicPartition,
      config,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      true,
      segments,
      0L,
      0L,
      leaderEpochCache.toJava,
      stateManager,
      new ConcurrentHashMap[String, Integer],
      false
    ).load()
    val localLog = new LocalLog(logDir, config, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, topicPartition,
      logDirFailureChannel)
    new UnifiedLog(offsets.logStartOffset,
      localLog,
      brokerTopicStats = brokerTopicStats,
      producerIdExpirationCheckIntervalMs = 30000,
      leaderEpochCache = leaderEpochCache,
      producerStateManager = stateManager,
      _topicId = None,
      keepPartitionMetadataFile = true)

    verify(stateManager).removeStraySnapshots(any[java.util.List[java.lang.Long]])
    verify(stateManager, times(2)).updateMapEndOffset(0L)
    verify(stateManager, times(2)).takeSnapshot()
    verify(stateManager).isEmpty
    verify(stateManager).firstUnstableOffset
  }

  @Test
  def testLoadProducersAfterDeleteRecordsMidSegment(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val pid2 = 2L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes)), producerId = pid2,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    assertEquals(2, log.activeProducersWithLastSequence.size)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(1L, LogStartOffsetIncrementReason.ClientRecordDeletion)

    // Deleting records should not remove producer state
    assertEquals(2, log.activeProducersWithLastSequence.size)
    val retainedLastSeqOpt = log.activeProducersWithLastSequence.get(pid2)
    assertTrue(retainedLastSeqOpt.isDefined)
    assertEquals(0, retainedLastSeqOpt.get)

    log.close()

    // Because the log start offset did not advance, producer snapshots will still be present and the state will be rebuilt
    val reloadedLog = createLog(logDir, logConfig, logStartOffset = 1L, lastShutdownClean = false)
    assertEquals(2, reloadedLog.activeProducersWithLastSequence.size)
    val reloadedLastSeqOpt = log.activeProducersWithLastSequence.get(pid2)
    assertEquals(retainedLastSeqOpt, reloadedLastSeqOpt)
  }

  @Test
  def testLoadingLogKeepsLargestStrayProducerStateSnapshot(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, retentionBytes = 0, retentionMs = 1000 * 60, fileDeleteDelayMs = 0)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)), producerId = pid1, producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid1, producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
    log.roll()

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)), producerId = pid1, producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("d".getBytes)), producerId = pid1, producerEpoch = epoch, sequence = 3), leaderEpoch = 0)

    // Close the log, we should now have 3 segments
    log.close()
    assertEquals(log.logSegments.size, 3)
    // We expect 3 snapshot files, two of which are for the first two segments, the last was written out during log closing.
    assertEquals(Seq(1L, 2L, 4L), ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted)
    // Inject a stray snapshot file within the bounds of the log at offset 3, it should be cleaned up after loading the log
    val straySnapshotFile = LogFileUtils.producerSnapshotFile(logDir, 3).toPath
    Files.createFile(straySnapshotFile)
    assertEquals(Seq(1L, 2L, 3L, 4L), ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted)

    createLog(logDir, logConfig, lastShutdownClean = false)
    // We should clean up the stray producer state snapshot file, but keep the largest snapshot file (4)
    assertEquals(Seq(1L, 2L, 4L), ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted)
  }

  @Test
  def testLoadProducersAfterDeleteRecordsOnSegment(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val pid2 = 2L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes)), producerId = pid2,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)

    assertEquals(2, log.logSegments.size)
    assertEquals(2, log.activeProducersWithLastSequence.size)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(1L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.deleteOldSegments()

    // Deleting records should not remove producer state
    assertEquals(1, log.logSegments.size)
    assertEquals(2, log.activeProducersWithLastSequence.size)
    val retainedLastSeqOpt = log.activeProducersWithLastSequence.get(pid2)
    assertTrue(retainedLastSeqOpt.isDefined)
    assertEquals(0, retainedLastSeqOpt.get)

    log.close()

    // After reloading log, producer state should not be regenerated
    val reloadedLog = createLog(logDir, logConfig, logStartOffset = 1L, lastShutdownClean = false)
    assertEquals(1, reloadedLog.activeProducersWithLastSequence.size)
    val reloadedEntryOpt = log.activeProducersWithLastSequence.get(pid2)
    assertEquals(retainedLastSeqOpt, reloadedEntryOpt)
  }

  /**
   * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
   */
  @Test
  def testLogRecoversToCorrectOffset(): Unit = {
    val numMessages = 100
    val messageSize = 100
    val segmentSize = 7 * messageSize
    val indexInterval = 3 * messageSize
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize, indexIntervalBytes = indexInterval, segmentIndexBytes = 4096)
    var log = createLog(logDir, logConfig)
    for (i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(messageSize),
        timestamp = mockTime.milliseconds + i * 10), leaderEpoch = 0)
    assertEquals(numMessages, log.logEndOffset,
      "After appending %d messages to an empty log, the log end offset should be %d".format(numMessages, numMessages))
    val lastIndexOffset = log.activeSegment.offsetIndex.lastOffset
    val numIndexEntries = log.activeSegment.offsetIndex.entries
    val lastOffset = log.logEndOffset
    // After segment is closed, the last entry in the time index should be (largest timestamp -> last offset).
    val lastTimeIndexOffset = log.logEndOffset - 1
    val lastTimeIndexTimestamp = log.activeSegment.largestTimestamp
    // Depending on when the last time index entry is inserted, an entry may or may not be inserted into the time index.
    val numTimeIndexEntries = log.activeSegment.timeIndex.entries + {
      if (log.activeSegment.timeIndex.lastEntry.offset == log.logEndOffset - 1) 0 else 1
    }
    log.close()

    def verifyRecoveredLog(log: UnifiedLog, expectedRecoveryPoint: Long): Unit = {
      assertEquals(expectedRecoveryPoint, log.recoveryPoint, s"Unexpected recovery point")
      assertEquals(numMessages, log.logEndOffset, s"Should have $numMessages messages when log is reopened w/o recovery")
      assertEquals(lastIndexOffset, log.activeSegment.offsetIndex.lastOffset, "Should have same last index offset as before.")
      assertEquals(numIndexEntries, log.activeSegment.offsetIndex.entries, "Should have same number of index entries as before.")
      assertEquals(lastTimeIndexTimestamp, log.activeSegment.timeIndex.lastEntry.timestamp, "Should have same last time index timestamp")
      assertEquals(lastTimeIndexOffset, log.activeSegment.timeIndex.lastEntry.offset, "Should have same last time index offset")
      assertEquals(numTimeIndexEntries, log.activeSegment.timeIndex.entries, "Should have same number of time index entries as before.")
    }

    log = createLog(logDir, logConfig, recoveryPoint = lastOffset, lastShutdownClean = false)
    verifyRecoveredLog(log, lastOffset)
    log.close()

    // test recovery case
    val recoveryPoint = 10
    log = createLog(logDir, logConfig, recoveryPoint = recoveryPoint, lastShutdownClean = false)
    // the recovery point should not be updated after unclean shutdown until the log is flushed
    verifyRecoveredLog(log, recoveryPoint)
    log.flush(false)
    verifyRecoveredLog(log, lastOffset)
    log.close()
  }

  /**
   * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testIndexRebuild(): Unit = {
    // publish the messages and close the log
    val numMessages = 200
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    var log = createLog(logDir, logConfig)
    for (i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = mockTime.milliseconds + i * 10), leaderEpoch = 0)
    val indexFiles = log.logSegments.asScala.map(_.offsetIndexFile)
    val timeIndexFiles = log.logSegments.asScala.map(_.timeIndexFile)
    log.close()

    // delete all the index files
    indexFiles.foreach(_.delete())
    timeIndexFiles.foreach(_.delete())

    // reopen the log
    log = createLog(logDir, logConfig, lastShutdownClean = false)
    assertEquals(numMessages, log.logEndOffset, "Should have %d messages when log is reopened".format(numMessages))
    assertTrue(log.logSegments.asScala.head.offsetIndex.entries > 0, "The index should have been rebuilt")
    assertTrue(log.logSegments.asScala.head.timeIndex.entries > 0, "The time index should have been rebuilt")
    for (i <- 0 until numMessages) {
      assertEquals(i, LogTestUtils.readLog(log, i, 100).records.batches.iterator.next().lastOffset)
      if (i == 0)
        assertEquals(log.logSegments.asScala.head.baseOffset,
          log.fetchOffsetByTimestamp(mockTime.milliseconds + i * 10).timestampAndOffsetOpt.get.offset)
      else
        assertEquals(i,
          log.fetchOffsetByTimestamp(mockTime.milliseconds + i * 10).timestampAndOffsetOpt.get.offset)
    }
    log.close()
  }

  /**
   * Test that if messages format version of the messages in a segment is before 0.10.0, the time index should be empty.
   */
  @nowarn("cat=deprecation")
  @Test
  def testRebuildTimeIndexForOldMessages(): Unit = {
    val numMessages = 200
    val segmentSize = 200
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, segmentSize.toString)
    logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "1")
    logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "0.9.0")
    val logConfig = new LogConfig(logProps)
    var log = createLog(logDir, logConfig)
    for (i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10),
        timestamp = mockTime.milliseconds + i * 10, magicValue = RecordBatch.MAGIC_VALUE_V1), leaderEpoch = 0)
    val timeIndexFiles = log.logSegments.asScala.map(_.timeIndexFile())
    log.close()

    // Delete the time index.
    timeIndexFiles.foreach(file => Files.delete(file.toPath))

    // The rebuilt time index should be empty
    log = createLog(logDir, logConfig, recoveryPoint = numMessages + 1, lastShutdownClean = false)
    for (segment <- log.logSegments.asScala.init) {
      assertEquals(0, segment.timeIndex.entries, "The time index should be empty")
      assertEquals(0, segment.timeIndexFile().length, "The time index file size should be 0")
    }
  }


  /**
   * Test that if we have corrupted an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testCorruptIndexRebuild(): Unit = {
    // publish the messages and close the log
    val numMessages = 200
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    var log = createLog(logDir, logConfig)
    for (i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = mockTime.milliseconds + i * 10), leaderEpoch = 0)
    val indexFiles = log.logSegments.asScala.map(_.offsetIndexFile())
    val timeIndexFiles = log.logSegments.asScala.map(_.timeIndexFile())
    log.close()

    // corrupt all the index files
    for ( file <- indexFiles) {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("  ")
      bw.close()
    }

    // corrupt all the index files
    for ( file <- timeIndexFiles) {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("  ")
      bw.close()
    }

    // reopen the log with recovery point=0 so that the segment recovery can be triggered
    log = createLog(logDir, logConfig, lastShutdownClean = false)
    assertEquals(numMessages, log.logEndOffset, "Should have %d messages when log is reopened".format(numMessages))
    for (i <- 0 until numMessages) {
      assertEquals(i, LogTestUtils.readLog(log, i, 100).records.batches.iterator.next().lastOffset)
      if (i == 0)
        assertEquals(log.logSegments.asScala.head.baseOffset,
          log.fetchOffsetByTimestamp(mockTime.milliseconds + i * 10).timestampAndOffsetOpt.get.offset)
      else
        assertEquals(i,
          log.fetchOffsetByTimestamp(mockTime.milliseconds + i * 10).timestampAndOffsetOpt.get.offset)
    }
    log.close()
  }

  /**
   * When we open a log any index segments without an associated log segment should be deleted.
   */
  @Test
  def testBogusIndexSegmentsAreRemoved(): Unit = {
    val bogusIndex1 = LogFileUtils.offsetIndexFile(logDir, 0)
    val bogusTimeIndex1 = LogFileUtils.timeIndexFile(logDir, 0)
    val bogusIndex2 = LogFileUtils.offsetIndexFile(logDir, 5)
    val bogusTimeIndex2 = LogFileUtils.timeIndexFile(logDir, 5)

    // The files remain absent until we first access it because we are doing lazy loading for time index and offset index
    // files but in this test case we need to create these files in order to test we will remove them.
    Files.createFile(bogusIndex2.toPath)
    Files.createFile(bogusTimeIndex2.toPath)

    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    // Force the segment to access the index files because we are doing index lazy loading.
    log.logSegments.asScala.head.offsetIndex
    log.logSegments.asScala.head.timeIndex

    assertTrue(bogusIndex1.length > 0,
      "The first index file should have been replaced with a larger file")
    assertTrue(bogusTimeIndex1.length > 0,
      "The first time index file should have been replaced with a larger file")
    assertFalse(bogusIndex2.exists,
      "The second index file should have been deleted.")
    assertFalse(bogusTimeIndex2.exists,
      "The second time index file should have been deleted.")

    // check that we can append to the log
    for (_ <- 0 until 10)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.delete()
  }

  /**
   * Verify that truncation works correctly after re-opening the log
   */
  @Test
  def testReopenThenTruncate(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    // create a log
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, indexIntervalBytes = 10000)
    var log = createLog(logDir, logConfig)

    // add enough messages to roll over several segments then close and re-open and attempt to truncate
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    log.close()
    log = createLog(logDir, logConfig, lastShutdownClean = false)
    log.truncateTo(3)
    assertEquals(1, log.numberOfSegments, "All but one segment should be deleted.")
    assertEquals(3, log.logEndOffset, "Log end offset should be 3.")
  }

  /**
   * Any files ending in .deleted should be removed when the log is re-opened.
   */
  @Test
  def testOpenDeletesObsoleteFiles(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds - 1000)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, retentionMs = 999)
    var log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    // expire all segments
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    log.close()
    log = createLog(logDir, logConfig, lastShutdownClean = false)
    assertEquals(1, log.numberOfSegments, "The deleted segments should be gone.")
  }

  @Test
  def testCorruptLog(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    val recoveryPoint = 50L
    for (_ <- 0 until 10) {
      // create a log and write some messages to it
      logDir.mkdirs()
      var log = createLog(logDir, logConfig)
      val numMessages = 50 + TestUtils.random.nextInt(50)
      for (_ <- 0 until numMessages)
        log.appendAsLeader(createRecords, leaderEpoch = 0)
      val records = log.logSegments.asScala.flatMap(_.log.records.asScala.toList).toList
      log.close()

      // corrupt index and log by appending random bytes
      TestUtils.appendNonsenseToFile(log.activeSegment.offsetIndexFile, TestUtils.random.nextInt(1024) + 1)
      TestUtils.appendNonsenseToFile(log.activeSegment.log.file, TestUtils.random.nextInt(1024) + 1)

      // attempt recovery
      log = createLog(logDir, logConfig, brokerTopicStats, 0L, recoveryPoint, lastShutdownClean = false)
      assertEquals(numMessages, log.logEndOffset)

      val recovered = log.logSegments.asScala.flatMap(_.log.records.asScala.toList).toList
      assertEquals(records.size, recovered.size)

      for (i <- records.indices) {
        val expected = records(i)
        val actual = recovered(i)
        assertEquals(expected.key, actual.key, s"Keys not equal")
        assertEquals(expected.value, actual.value, s"Values not equal")
        assertEquals(expected.timestamp, actual.timestamp, s"Timestamps not equal")
      }

      Utils.delete(logDir)
    }
  }

  @Test
  def testOverCompactedLogRecovery(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    val set1 = MemoryRecords.withRecords(0, Compression.NONE, 0, new SimpleRecord("v1".getBytes(), "k1".getBytes()))
    val set2 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 2, Compression.NONE, 0, new SimpleRecord("v3".getBytes(), "k3".getBytes()))
    val set3 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 3, Compression.NONE, 0, new SimpleRecord("v4".getBytes(), "k4".getBytes()))
    val set4 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 4, Compression.NONE, 0, new SimpleRecord("v5".getBytes(), "k5".getBytes()))
    //Writes into an empty log with baseOffset 0
    log.appendAsFollower(set1)
    assertEquals(0L, log.activeSegment.baseOffset)
    //This write will roll the segment, yielding a new segment with base offset = max(1, Integer.MAX_VALUE+2) = Integer.MAX_VALUE+2
    log.appendAsFollower(set2)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    assertTrue(LogFileUtils.producerSnapshotFile(logDir, Integer.MAX_VALUE.toLong + 2).exists)
    //This will go into the existing log
    log.appendAsFollower(set3)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    //This will go into the existing log
    log.appendAsFollower(set4)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    log.close()
    val indexFiles = logDir.listFiles.filter(file => file.getName.contains(".index"))
    assertEquals(2, indexFiles.length)
    for (file <- indexFiles) {
      val offsetIndex = new OffsetIndex(file, file.getName.replace(".index","").toLong)
      assertTrue(offsetIndex.lastOffset >= 0)
      offsetIndex.close()
    }
    Utils.delete(logDir)
  }

  @nowarn("cat=deprecation")
  @Test
  def testLeaderEpochCacheClearedAfterStaticMessageFormatDowngrade(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 65536)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), leaderEpoch = 5)
    assertEquals(Some(5), log.latestEpoch)
    log.close()

    // reopen the log with an older message format version and check the cache
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1000")
    logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "1")
    logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "65536")
    logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, "0.10.2")
    val downgradedLogConfig = new LogConfig(logProps)
    val reopened = createLog(logDir, downgradedLogConfig, lastShutdownClean = false)
    LogTestUtils.assertLeaderEpochCacheEmpty(reopened)

    reopened.appendAsLeader(TestUtils.records(List(new SimpleRecord("bar".getBytes())),
      magicValue = RecordVersion.V1.value), leaderEpoch = 5)
    LogTestUtils.assertLeaderEpochCacheEmpty(reopened)
  }

  @Test
  def testOverCompactedLogRecoveryMultiRecord(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    val set1 = MemoryRecords.withRecords(0, Compression.NONE, 0, new SimpleRecord("v1".getBytes(), "k1".getBytes()))
    val set2 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 2, Compression.gzip().build(), 0,
      new SimpleRecord("v3".getBytes(), "k3".getBytes()),
      new SimpleRecord("v4".getBytes(), "k4".getBytes()))
    val set3 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 4, Compression.gzip().build(), 0,
      new SimpleRecord("v5".getBytes(), "k5".getBytes()),
      new SimpleRecord("v6".getBytes(), "k6".getBytes()))
    val set4 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 6, Compression.gzip().build(), 0,
      new SimpleRecord("v7".getBytes(), "k7".getBytes()),
      new SimpleRecord("v8".getBytes(), "k8".getBytes()))
    //Writes into an empty log with baseOffset 0
    log.appendAsFollower(set1)
    assertEquals(0L, log.activeSegment.baseOffset)
    //This write will roll the segment, yielding a new segment with base offset = max(1, Integer.MAX_VALUE+2) = Integer.MAX_VALUE+2
    log.appendAsFollower(set2)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    assertTrue(LogFileUtils.producerSnapshotFile(logDir, Integer.MAX_VALUE.toLong + 2).exists)
    //This will go into the existing log
    log.appendAsFollower(set3)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    //This will go into the existing log
    log.appendAsFollower(set4)
    assertEquals(Integer.MAX_VALUE.toLong + 2, log.activeSegment.baseOffset)
    log.close()
    val indexFiles = logDir.listFiles.filter(file => file.getName.contains(".index"))
    assertEquals(2, indexFiles.length)
    for (file <- indexFiles) {
      val offsetIndex = new OffsetIndex(file, file.getName.replace(".index","").toLong)
      assertTrue(offsetIndex.lastOffset >= 0)
      offsetIndex.close()
    }
    Utils.delete(logDir)
  }

  @Test
  def testOverCompactedLogRecoveryMultiRecordV1(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    val set1 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0, Compression.NONE,
      new SimpleRecord("v1".getBytes(), "k1".getBytes()))
    val set2 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, Integer.MAX_VALUE.toLong + 2, Compression.gzip().build(),
      new SimpleRecord("v3".getBytes(), "k3".getBytes()),
      new SimpleRecord("v4".getBytes(), "k4".getBytes()))
    val set3 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, Integer.MAX_VALUE.toLong + 4, Compression.gzip().build(),
      new SimpleRecord("v5".getBytes(), "k5".getBytes()),
      new SimpleRecord("v6".getBytes(), "k6".getBytes()))
    val set4 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, Integer.MAX_VALUE.toLong + 6, Compression.gzip().build(),
      new SimpleRecord("v7".getBytes(), "k7".getBytes()),
      new SimpleRecord("v8".getBytes(), "k8".getBytes()))
    //Writes into an empty log with baseOffset 0
    log.appendAsFollower(set1)
    assertEquals(0L, log.activeSegment.baseOffset)
    //This write will roll the segment, yielding a new segment with base offset = max(1, 3) = 3
    log.appendAsFollower(set2)
    assertEquals(3, log.activeSegment.baseOffset)
    assertTrue(LogFileUtils.producerSnapshotFile(logDir, 3).exists)
    //This will also roll the segment, yielding a new segment with base offset = max(5, Integer.MAX_VALUE+4) = Integer.MAX_VALUE+4
    log.appendAsFollower(set3)
    assertEquals(Integer.MAX_VALUE.toLong + 4, log.activeSegment.baseOffset)
    assertTrue(LogFileUtils.producerSnapshotFile(logDir, Integer.MAX_VALUE.toLong + 4).exists)
    //This will go into the existing log
    log.appendAsFollower(set4)
    assertEquals(Integer.MAX_VALUE.toLong + 4, log.activeSegment.baseOffset)
    log.close()
    val indexFiles = logDir.listFiles.filter(file => file.getName.contains(".index"))
    assertEquals(3, indexFiles.length)
    for (file <- indexFiles) {
      val offsetIndex = new OffsetIndex(file, file.getName.replace(".index","").toLong)
      assertTrue(offsetIndex.lastOffset >= 0)
      offsetIndex.close()
    }
    Utils.delete(logDir)
  }

  @Test
  def testRecoveryOfSegmentWithOffsetOverflow(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, _) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)

    // Run recovery on the log. This should split the segment underneath. Ignore .deleted files as we could have still
    // have them lying around after the split.
    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))

    // Running split again would throw an error

    for (segment <- recoveredLog.logSegments.asScala) {
      assertThrows(classOf[IllegalArgumentException], () => log.splitOverflowedSegment(segment))
    }
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase1(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery just after .cleaned file is created, before rename to .swap. On recovery, existing split
    // operation is aborted but the recovery process itself kicks off split which should complete.
    newSegments.reverse.foreach(segment => {
      segment.changeFileSuffixes("", UnifiedLog.CleanedFileSuffix)
      segment.truncateTo(0)
    })
    for (file <- logDir.listFiles if file.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX))
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(Utils.replaceSuffix(file.getPath, LogFileUtils.DELETED_FILE_SUFFIX, "")))

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    recoveredLog.close()
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase2(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery just after one of the new segments has been renamed to .swap. On recovery, existing split
    // operation is aborted but the recovery process itself kicks off split which should complete.
    newSegments.reverse.foreach { segment =>
      if (segment != newSegments.last)
        segment.changeFileSuffixes("", UnifiedLog.CleanedFileSuffix)
      else
        segment.changeFileSuffixes("", UnifiedLog.SwapFileSuffix)
      segment.truncateTo(0)
    }
    for (file <- logDir.listFiles if file.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX))
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(Utils.replaceSuffix(file.getPath, LogFileUtils.DELETED_FILE_SUFFIX, "")))

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    recoveredLog.close()
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase3(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery right after all new segments have been renamed to .swap. On recovery, existing split operation
    // is completed and the old segment must be deleted.
    newSegments.reverse.foreach(segment => {
      segment.changeFileSuffixes("", UnifiedLog.SwapFileSuffix)
    })
    for (file <- logDir.listFiles if file.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX))
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(Utils.replaceSuffix(file.getPath, LogFileUtils.DELETED_FILE_SUFFIX, "")))

    // Truncate the old segment
    segmentWithOverflow.truncateTo(0)

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    log.close()
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase4(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery right after all new segments have been renamed to .swap and old segment has been deleted. On
    // recovery, existing split operation is completed.
    newSegments.reverse.foreach(_.changeFileSuffixes("", UnifiedLog.SwapFileSuffix))

    for (file <- logDir.listFiles if file.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX))
      Utils.delete(file)

    // Truncate the old segment
    segmentWithOverflow.truncateTo(0)

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    recoveredLog.close()
  }

  @Test
  def testRecoveryAfterCrashDuringSplitPhase5(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    val expectedKeys = LogTestUtils.keysInLog(log)
    val numSegmentsInitial = log.logSegments.size

    // Split the segment
    val newSegments = log.splitOverflowedSegment(segmentWithOverflow)

    // Simulate recovery right after one of the new segment has been renamed to .swap and the other to .log. On
    // recovery, existing split operation is completed.
    newSegments.last.changeFileSuffixes("", UnifiedLog.SwapFileSuffix)

    // Truncate the old segment
    segmentWithOverflow.truncateTo(0)

    val recoveredLog = recoverAndCheck(logConfig, expectedKeys)
    assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog))
    assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments.size)
    recoveredLog.close()
  }

  @Test
  def testCleanShutdownFile(): Unit = {
    // append some messages to create some segments
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)

    var recoveryPoint = 0L
    // create a log and write some messages to it
    var log = createLog(logDir, logConfig)
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    log.close()

    // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
    // clean shutdown file exists. Note: Earlier, Log layer relied on the presence of clean shutdown file to determine the status
    // of last shutdown. Now, LogManager checks for the presence of this file and immediately deletes the same. It passes
    // down a clean shutdown flag to the Log layer as log is loaded. Recovery is attempted based on this flag.
    recoveryPoint = log.logEndOffset
    log = createLog(logDir, logConfig)
    assertEquals(recoveryPoint, log.logEndOffset)
  }

  /**
   * Append a bunch of messages to a log and then re-open it with recovery and check that the leader epochs are recovered properly.
   */
  @Test
  def testLogRecoversForLeaderEpoch(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))
    val leaderEpochCache = log.leaderEpochCache.get
    val firstBatch = singletonRecordsWithLeaderEpoch(value = "random".getBytes, leaderEpoch = 1, offset = 0)
    log.appendAsFollower(records = firstBatch)

    val secondBatch = singletonRecordsWithLeaderEpoch(value = "random".getBytes, leaderEpoch = 2, offset = 1)
    log.appendAsFollower(records = secondBatch)

    val thirdBatch = singletonRecordsWithLeaderEpoch(value = "random".getBytes, leaderEpoch = 2, offset = 2)
    log.appendAsFollower(records = thirdBatch)

    val fourthBatch = singletonRecordsWithLeaderEpoch(value = "random".getBytes, leaderEpoch = 3, offset = 3)
    log.appendAsFollower(records = fourthBatch)

    assertEquals(java.util.Arrays.asList(new EpochEntry(1, 0), new EpochEntry(2, 1), new EpochEntry(3, 3)), leaderEpochCache.epochEntries)

    // deliberately remove some of the epoch entries
    leaderEpochCache.truncateFromEndAsyncFlush(2)
    assertNotEquals(java.util.Arrays.asList(new EpochEntry(1, 0), new EpochEntry(2, 1), new EpochEntry(3, 3)), leaderEpochCache.epochEntries)
    log.close()

    // reopen the log and recover from the beginning
    val recoveredLog = createLog(logDir, new LogConfig(new Properties), lastShutdownClean = false)
    val recoveredLeaderEpochCache = recoveredLog.leaderEpochCache.get

    // epoch entries should be recovered
    assertEquals(java.util.Arrays.asList(new EpochEntry(1, 0), new EpochEntry(2, 1), new EpochEntry(3, 3)), recoveredLeaderEpochCache.epochEntries)
    recoveredLog.close()
  }

  @Test
  def testFullTransactionIndexRecovery(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 128 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime)
    val appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime)
    val appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime)
    val appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    LogTestUtils.appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    LogTestUtils.appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 75
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 90

    // delete all the offset and transaction index files to force recovery
    log.logSegments.forEach { segment =>
      segment.offsetIndex.deleteIfExists()
      segment.txnIndex.deleteIfExists()
    }

    log.close()

    val reloadedLogConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 5)
    val reloadedLog = createLog(logDir, reloadedLogConfig, lastShutdownClean = false)
    val abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
  }

  @Test
  def testRecoverOnlyLastSegment(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 128 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime)
    val appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime)
    val appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime)
    val appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    LogTestUtils.appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    LogTestUtils.appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 75
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 90

    // delete the last offset and transaction index files to force recovery
    val lastSegment = log.logSegments.asScala.last
    val recoveryPoint = lastSegment.baseOffset
    lastSegment.offsetIndex.deleteIfExists()
    lastSegment.txnIndex.deleteIfExists()

    log.close()

    val reloadedLogConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 5)
    val reloadedLog = createLog(logDir, reloadedLogConfig, recoveryPoint = recoveryPoint, lastShutdownClean = false)
    val abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
  }

  @Test
  def testRecoverLastSegmentWithNoSnapshots(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 128 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime)
    val appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime)
    val appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime)
    val appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    LogTestUtils.appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    LogTestUtils.appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 75
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 90

    LogTestUtils.deleteProducerSnapshotFiles(logDir)

    // delete the last offset and transaction index files to force recovery. this should force us to rebuild
    // the producer state from the start of the log
    val lastSegment = log.logSegments.asScala.last
    val recoveryPoint = lastSegment.baseOffset
    lastSegment.offsetIndex.deleteIfExists()
    lastSegment.txnIndex.deleteIfExists()

    log.close()

    val reloadedLogConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 5)
    val reloadedLog = createLog(logDir, reloadedLogConfig, recoveryPoint = recoveryPoint, lastShutdownClean = false)
    val abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog)
    assertEquals(List(new AbortedTxn(pid1, 0L, 29L, 8L), new AbortedTxn(pid2, 8L, 74L, 36L)), abortedTransactions)
  }

  @Test
  def testLogEndLessThanStartAfterReopen(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    var log = createLog(logDir, logConfig)
    for (i <- 0 until 5) {
      val record = new SimpleRecord(mockTime.milliseconds, i.toString.getBytes)
      log.appendAsLeader(TestUtils.records(List(record)), leaderEpoch = 0)
      log.roll()
    }
    assertEquals(6, log.logSegments.size)

    // Increment the log start offset
    val startOffset = 4
    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(startOffset, LogStartOffsetIncrementReason.ClientRecordDeletion)
    assertTrue(log.logEndOffset > log.logStartOffset)

    // Append garbage to a segment below the current log start offset
    val segmentToForceTruncation = log.logSegments.asScala.take(2).last
    val bw = new BufferedWriter(new FileWriter(segmentToForceTruncation.log.file))
    bw.write("corruptRecord")
    bw.close()
    log.close()

    // Reopen the log. This will cause truncate the segment to which we appended garbage and delete all other segments.
    // All remaining segments will be lower than the current log start offset, which will force deletion of all segments
    // and recreation of a single, active segment starting at logStartOffset.
    log = createLog(logDir, logConfig, logStartOffset = startOffset, lastShutdownClean = false)
    // Wait for segment deletions (if any) to complete.
    mockTime.sleep(logConfig.fileDeleteDelayMs)
    assertEquals(1, log.numberOfSegments)
    assertEquals(startOffset, log.logStartOffset)
    assertEquals(startOffset, log.logEndOffset)
    // Validate that the remaining segment matches our expectations
    val onlySegment = log.logSegments.asScala.head
    assertEquals(startOffset, onlySegment.baseOffset)
    assertTrue(onlySegment.log.file().exists())
    assertTrue(onlySegment.offsetIndexFile().exists())
    assertTrue(onlySegment.timeIndexFile().exists())
  }

  @Test
  def testCorruptedLogRecoveryDoesNotDeleteProducerStateSnapshotsPostRecovery(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    var log = createLog(logDir, logConfig)
    // Create segments: [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
    //                   |---> logStartOffset                                           |---> active segment (empty)
    //                                                                                  |---> logEndOffset
    for (i <- 0 until 9) {
      val record = new SimpleRecord(mockTime.milliseconds, i.toString.getBytes)
      log.appendAsLeader(TestUtils.records(List(record)), leaderEpoch = 0)
      log.roll()
    }
    assertEquals(10, log.logSegments.size)
    assertEquals(0, log.logStartOffset)
    assertEquals(9, log.activeSegment.baseOffset)
    assertEquals(9, log.logEndOffset)
    for (offset <- 1 until 10) {
      val snapshotFileBeforeDeletion = log.producerStateManager.snapshotFileForOffset(offset).toScala
      assertTrue(snapshotFileBeforeDeletion.isDefined)
      assertTrue(snapshotFileBeforeDeletion.get.file.exists)
    }

    // Increment the log start offset to 4.
    // After this step, the segments should be:
    //                              |---> logStartOffset
    // [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
    //                                                                 |---> active segment (empty)
    //                                                                 |---> logEndOffset
    val newLogStartOffset = 4
    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(newLogStartOffset, LogStartOffsetIncrementReason.ClientRecordDeletion)
    assertEquals(4, log.logStartOffset)
    assertEquals(9, log.logEndOffset)

    // Append garbage to a segment at baseOffset 1, which is below the current log start offset 4.
    // After this step, the segments should be:
    //
    // [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
    //           |                  |---> logStartOffset               |---> active segment  (empty)
    //           |                                                     |---> logEndOffset
    // corrupt record inserted
    //
    val segmentToForceTruncation = log.logSegments.asScala.take(2).last
    assertEquals(1, segmentToForceTruncation.baseOffset)
    val bw = new BufferedWriter(new FileWriter(segmentToForceTruncation.log.file))
    bw.write("corruptRecord")
    bw.close()
    log.close()

    // Reopen the log. This will do the following:
    // - Truncate the segment above to which we appended garbage and will schedule async deletion of all other
    //   segments from base offsets 2 to 9.
    // - The remaining segments at base offsets 0 and 1 will be lower than the current logStartOffset 4.
    //   This will cause async deletion of both remaining segments. Finally a single, active segment is created
    //   starting at logStartOffset 4.
    //
    // Expected segments after the log is opened again:
    // [4-]
    //  |---> active segment (empty)
    //  |---> logStartOffset
    //  |---> logEndOffset
    log = createLog(logDir, logConfig, logStartOffset = newLogStartOffset, lastShutdownClean = false)
    assertEquals(1, log.logSegments.size)
    assertEquals(4, log.logStartOffset)
    assertEquals(4, log.activeSegment.baseOffset)
    assertEquals(4, log.logEndOffset)

    val offsetsWithSnapshotFiles = (1 until 5)
        .map(offset => new SnapshotFile(LogFileUtils.producerSnapshotFile(logDir, offset)))
        .filter(snapshotFile => snapshotFile.file.exists())
        .map(_.offset)
    val inMemorySnapshotFiles = (1 until 5)
        .flatMap(offset => log.producerStateManager.snapshotFileForOffset(offset).toScala)

    assertTrue(offsetsWithSnapshotFiles.isEmpty, s"Found offsets with producer state snapshot files: $offsetsWithSnapshotFiles while none were expected.")
    assertTrue(inMemorySnapshotFiles.isEmpty, s"Found in-memory producer state snapshot files: $inMemorySnapshotFiles while none were expected.")

    // Append records, roll the segments and check that the producer state snapshots are defined.
    // The expected segments and producer state snapshots, after the appends are complete and segments are rolled,
    // is as shown below:
    // [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
    //  |      |      |      |      |      |---> active segment (empty)
    //  |      |      |      |      |      |---> logEndOffset
    //  |      |      |      |      |      |
    //  |      |------.------.------.------.-----> producer state snapshot files are DEFINED for each offset in: [5-9]
    //  |----------------------------------------> logStartOffset
    for (i <- 0 until 5) {
      val record = new SimpleRecord(mockTime.milliseconds, i.toString.getBytes)
      log.appendAsLeader(TestUtils.records(List(record)), leaderEpoch = 0)
      log.roll()
    }
    assertEquals(9, log.activeSegment.baseOffset)
    assertEquals(9, log.logEndOffset)
    for (offset <- 5 until 10) {
      val snapshotFileBeforeDeletion = log.producerStateManager.snapshotFileForOffset(offset)
      assertTrue(snapshotFileBeforeDeletion.isPresent)
      assertTrue(snapshotFileBeforeDeletion.get.file.exists)
    }

    // Wait for all async segment deletions scheduled during Log recovery to complete.
    // The expected segments and producer state snapshot after the deletions, is as shown below:
    // [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
    //  |      |      |      |      |      |---> active segment (empty)
    //  |      |      |      |      |      |---> logEndOffset
    //  |      |      |      |      |      |
    //  |      |------.------.------.------.-----> producer state snapshot files should be defined for each offset in: [5-9].
    //  |----------------------------------------> logStartOffset
    mockTime.sleep(logConfig.fileDeleteDelayMs)
    assertEquals(newLogStartOffset, log.logStartOffset)
    assertEquals(9, log.logEndOffset)
    val offsetsWithMissingSnapshotFiles = ListBuffer[Long]()
    for (offset <- 5 until 10) {
      val snapshotFile = log.producerStateManager.snapshotFileForOffset(offset)
      if (!snapshotFile.isPresent || !snapshotFile.get.file.exists) {
        offsetsWithMissingSnapshotFiles.append(offset)
      }
    }
    assertTrue(offsetsWithMissingSnapshotFiles.isEmpty,
      s"Found offsets with missing producer state snapshot files: $offsetsWithMissingSnapshotFiles")
    assertFalse(logDir.list().exists(_.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)), "Expected no files to be present with the deleted file suffix")
  }

  @Test
  def testRecoverWithEmptyActiveSegment(): Unit = {
    val numMessages = 100
    val messageSize = 100
    val segmentSize = 7 * messageSize
    val indexInterval = 3 * messageSize
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize, indexIntervalBytes = indexInterval, segmentIndexBytes = 4096)
    var log = createLog(logDir, logConfig)
    for (i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(messageSize),
        timestamp = mockTime.milliseconds + i * 10), leaderEpoch = 0)
    assertEquals(numMessages, log.logEndOffset,
      "After appending %d messages to an empty log, the log end offset should be %d".format(numMessages, numMessages))
    log.roll()
    log.flush(false)
    assertThrows(classOf[NoSuchFileException], () => log.activeSegment.sanityCheck(true))
    var lastOffset = log.logEndOffset
    log.closeHandlers()

    log = createLog(logDir, logConfig, recoveryPoint = lastOffset, lastShutdownClean = false)
    assertEquals(lastOffset, log.recoveryPoint, s"Unexpected recovery point")
    assertEquals(numMessages, log.logEndOffset, s"Should have $numMessages messages when log is reopened w/o recovery")
    assertEquals(0, log.activeSegment.timeIndex.entries, "Should have same number of time index entries as before.")
    log.activeSegment.sanityCheck(true) // this should not throw because the LogLoader created the empty active log index file during recovery

    for (i <- 0 until numMessages)
      log.appendAsLeader(TestUtils.singletonRecords(value = TestUtils.randomBytes(messageSize),
        timestamp = mockTime.milliseconds + i * 10), leaderEpoch = 0)
    log.roll()
    assertThrows(classOf[NoSuchFileException], () => log.activeSegment.sanityCheck(true))
    log.flush(true)
    log.activeSegment.sanityCheck(true) // this should not throw because we flushed the active segment which created the empty log index file
    lastOffset = log.logEndOffset

    log = createLog(logDir, logConfig, recoveryPoint = lastOffset, lastShutdownClean = false)
    assertEquals(lastOffset, log.recoveryPoint, s"Unexpected recovery point")
    assertEquals(2 * numMessages, log.logEndOffset, s"Should have $numMessages messages when log is reopened w/o recovery")
    assertEquals(0, log.activeSegment.timeIndex.entries, "Should have same number of time index entries as before.")
    log.activeSegment.sanityCheck(true) // this should not throw

    log.close()
  }

  @ParameterizedTest
  @CsvSource(Array("false, 5", "true, 0"))
  def testLogStartOffsetWhenRemoteStorageIsEnabled(isRemoteLogEnabled: Boolean,
                                                   expectedLogStartOffset: Long): Unit = {
    val logDirFailureChannel = null
    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    val logConfig = LogTestUtils.createLogConfig()
    val stateManager: ProducerStateManager = mock(classOf[ProducerStateManager])
    when(stateManager.isEmpty).thenReturn(true)

    val log = createLog(logDir, logConfig)
    // Create segments: [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
    //                   |---> logStartOffset                                           |---> active segment (empty)
    //                                                                                  |---> logEndOffset
    for (i <- 0 until 9) {
      val record = new SimpleRecord(mockTime.milliseconds, i.toString.getBytes)
      log.appendAsLeader(TestUtils.records(List(record)), leaderEpoch = 0)
      log.roll()
    }
    log.maybeIncrementHighWatermark(new LogOffsetMetadata(9L))
    log.maybeIncrementLogStartOffset(5L, LogStartOffsetIncrementReason.SegmentDeletion)
    log.deleteOldSegments()

    val segments = new LogSegments(topicPartition)
    log.logSegments.forEach(segment => segments.add(segment))
    assertEquals(5, segments.firstSegment.get.baseOffset)

    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
      logDir, topicPartition, logDirFailureChannel, logConfig.recordVersion, "", None, mockTime.scheduler)
    val offsets = new LogLoader(
      logDir,
      topicPartition,
      logConfig,
      mockTime.scheduler,
      mockTime,
      logDirFailureChannel,
      true,
      segments,
      0L,
      0L,
      leaderEpochCache.toJava,
      stateManager,
      new ConcurrentHashMap[String, Integer],
      isRemoteLogEnabled
    ).load()
    assertEquals(expectedLogStartOffset, offsets.logStartOffset)
  }
}
